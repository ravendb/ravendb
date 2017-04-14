using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Replication;
using Raven.Client.Util;
using Raven.Server.Utils.Stats;

namespace Raven.Server.Documents.Replication
{
    public class IncomingReplicationStatsAggregator : StatsAggregator<IncomingReplicationRunStats, IncomingReplicationStatsScope>
    {
        private volatile IncomingReplicationPerformanceStats _performanceStats;

        public IncomingReplicationStatsAggregator(int id, StatsAggregator<IncomingReplicationRunStats, IncomingReplicationStatsScope> lastStats)
            : base(id, lastStats)
        {
        }

        public override IncomingReplicationStatsScope CreateScope()
        {
            Debug.Assert(Scope == null);

            return Scope = new IncomingReplicationStatsScope(Stats);
        }

        public IncomingReplicationPerformanceStats ToReplicationPerformanceLiveStatsWithDetails()
        {
            if (_performanceStats != null)
                return _performanceStats;

            if (Scope == null || Stats == null)
                return null;

            if (Completed)
                return ToReplicationPerformanceStats();

            return CreateIndexingPerformanceStats(completed: false);
        }

        public IncomingReplicationPerformanceStats ToReplicationPerformanceStats()
        {
            if (_performanceStats != null)
                return _performanceStats;

            lock (Stats)
            {
                if (_performanceStats != null)
                    return _performanceStats;

                return _performanceStats = CreateIndexingPerformanceStats(completed: true);
            }
        }

        private IncomingReplicationPerformanceStats CreateIndexingPerformanceStats(bool completed)
        {
            return new IncomingReplicationPerformanceStats(Scope.Duration)
            {
                Id = Id,
                Started = StartTime,
                Completed = completed ? StartTime.Add(Scope.Duration) : (DateTime?)null,
                Details = Scope.ToReplicationPerformanceOperation("Replication"),
                ReceivedLastEtag = Stats.LastEtag,
                Network = new IncomingReplicationPerformanceStats.NetworkStats
                {
                    InputCount = Stats.InputCount,
                    AttachmentReadCount = Stats.AttachmentReadCount,
                    DocumentReadCount = Stats.DocumentReadCount,
                    DocumentTombstoneReadCount = Stats.DocumentTombstoneReadCount,
                    AttachmentTombstoneReadCount = Stats.AttachmentTombstoneReadCount
                },
                Errors = Stats.Errors
            };
        }
    }

    public class IncomingReplicationStatsScope : StatsScope<IncomingReplicationRunStats, IncomingReplicationStatsScope>
    {
        private readonly IncomingReplicationRunStats _stats;

        public IncomingReplicationStatsScope(IncomingReplicationRunStats stats, bool start = true)
            : base(stats, start)
        {
            _stats = stats;
        }

        protected override IncomingReplicationStatsScope OpenNewScope(IncomingReplicationRunStats stats, bool start)
        {
            return new IncomingReplicationStatsScope(stats, start);
        }

        public ReplicationPerformanceOperation ToReplicationPerformanceOperation(string name)
        {
            var operation = new ReplicationPerformanceOperation(Duration)
            {
                Name = name
            };

            if (Scopes != null)
            {
                operation.Operations = Scopes
                    .Select(x => x.Value.ToReplicationPerformanceOperation(x.Key))
                    .ToArray();
            }

            return operation;
        }

        public void RecordDocumentRead()
        {
            _stats.DocumentReadCount++;
        }

        public void RecordDocumentTombstoneRead()
        {
            _stats.DocumentTombstoneReadCount++;
        }

        public void RecordAttachmentTombstoneRead()
        {
            _stats.AttachmentTombstoneReadCount++;
        }

        public void RecordAttachmentRead()
        {
            _stats.AttachmentReadCount++;
        }

        public void RecordInputAttempt()
        {
            _stats.InputCount++;
        }

        public void RecordLastEtag(long etag)
        {
            _stats.LastEtag = etag;
        }

        public void AddError(Exception exception)
        {
            _stats.AddError(exception);
        }
    }

    public class IncomingReplicationRunStats : ReplicationRunStatsBase
    {
        public int InputCount;

        public long LastEtag;

        public int DocumentReadCount;
        public int DocumentTombstoneReadCount;
        public int AttachmentTombstoneReadCount;
        public int AttachmentReadCount;
    }
}