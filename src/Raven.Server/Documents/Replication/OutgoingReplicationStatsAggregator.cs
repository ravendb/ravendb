using System;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Replication;
using Raven.Server.Utils.Stats;
using Sparrow;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingReplicationStatsAggregator : StatsAggregator<OutgoingReplicationRunStats, OutgoingReplicationStatsScope>
    {
        private volatile OutgoingReplicationPerformanceStats _performanceStats;

        public OutgoingReplicationStatsAggregator(int id, StatsAggregator<OutgoingReplicationRunStats, OutgoingReplicationStatsScope> lastStats)
            : base(id, lastStats)
        {
        }

        public override OutgoingReplicationStatsScope CreateScope()
        {
            Debug.Assert(Scope == null);

            return Scope = new OutgoingReplicationStatsScope(Stats);
        }

        public OutgoingReplicationPerformanceStats ToReplicationPerformanceLiveStatsWithDetails()
        {
            if (_performanceStats != null)
                return _performanceStats;

            if (Scope == null || Stats == null)
                return null;

            if (Completed)
                return ToReplicationPerformanceStats();

            return CreateIndexingPerformanceStats(completed: false);
        }

        public OutgoingReplicationPerformanceStats ToReplicationPerformanceStats()
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

        private OutgoingReplicationPerformanceStats CreateIndexingPerformanceStats(bool completed)
        {
            return new OutgoingReplicationPerformanceStats(Scope.Duration)
            {
                Id = Id,
                Started = StartTime,
                Completed = completed ? StartTime.Add(Scope.Duration) : (DateTime?)null,
                Details = Scope.ToReplicationPerformanceOperation("Replication"),
                SendLastEtag = Stats.LastEtag,
                Storage = new OutgoingReplicationPerformanceStats.StorageStats
                {
                    InputCount = Stats.InputCount,
                    ArtificialDocumentSkipCount = Stats.ArtificialDocumentSkipCount,
                    SystemDocumentSkipCount = Stats.SystemDocumentSkipCount,
                    ChangeVectorSkipCount = Stats.ChangeVectorSkipCount
                },
                Network = new OutgoingReplicationPerformanceStats.NetworkStats
                {
                    AttachmentOutputCount = Stats.AttachmentOutputCount,
                    AttachmentOutputSizeInBytes = Stats.AttachmentOutputSize.GetValue(SizeUnit.Bytes),
                    DocumentOutputCount = Stats.DocumentOutputCount,
                    DocumentOutputSizeInBytes = Stats.DocumentOutputSize.GetValue(SizeUnit.Bytes),
                    AttachmentTombstoneOutputCount = Stats.AttachmentTombstoneOutputCount,
                    DocumentTombstoneOutputCount = Stats.DocumentTombstoneOutputCount,
                    CounterOutputCount = Stats.CounterOutputCount,
                    CounterOutputSizeInBytes = Stats.CounterOutputSize.GetValue(SizeUnit.Bytes)
                },
                Errors = Stats.Errors
            };
        }
    }

    public class OutgoingReplicationStatsScope : StatsScope<OutgoingReplicationRunStats, OutgoingReplicationStatsScope>
    {
        private readonly OutgoingReplicationRunStats _stats;

        public OutgoingReplicationStatsScope(OutgoingReplicationRunStats stats, bool start = true)
            : base(stats, start)
        {
            _stats = stats;
        }

        protected override OutgoingReplicationStatsScope OpenNewScope(OutgoingReplicationRunStats stats, bool start)
        {
            return new OutgoingReplicationStatsScope(stats, start);
        }

        public void RecordInputAttempt()
        {
            _stats.InputCount++;
        }

        public void RecordArtificialDocumentSkip()
        {
            _stats.ArtificialDocumentSkipCount++;
        }

        public void RecordSystemDocumentSkip()
        {
            _stats.SystemDocumentSkipCount++;
        }

        public void RecordChangeVectorSkip()
        {
            _stats.ChangeVectorSkipCount++;
        }

        public void RecordAttachmentOutput(long sizeInBytes)
        {
            _stats.AttachmentOutputCount++;
            _stats.AttachmentOutputSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordAttachmentTombstoneOutput()
        {
            _stats.AttachmentTombstoneOutputCount++;
        }

        public void RecordRevisionTombstoneOutput()
        {
            _stats.RevisionTombstoneOutputCount++;
        }

        public void RecordDocumentOutput(long sizeInBytes)
        {
            _stats.DocumentOutputCount++;
            _stats.DocumentOutputSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordDocumentTombstoneOutput()
        {
            _stats.DocumentTombstoneOutputCount++;
        }

        public void RecordCounterOutput()
        {
            _stats.CounterOutputCount++;
        }

        public void RecordCountersOutput(int numberOfCounters)
        {
            _stats.CounterOutputCount += numberOfCounters;
        }

        public void RecordCounterTombstoneOutput()
        {
            _stats.CounterTombstoneOutputCount++;
        }

        public void RecordLastEtag(long etag)
        {
            _stats.LastEtag = etag;
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

        public void AddError(Exception exception)
        {
            _stats.AddError(exception);
        }
    }

    public class OutgoingReplicationRunStats : ReplicationRunStatsBase
    {
        public long LastEtag;

        public int InputCount;

        public int ArtificialDocumentSkipCount;
        public int SystemDocumentSkipCount;
        public int ChangeVectorSkipCount;

        public int AttachmentOutputCount;
        public Size AttachmentOutputSize;

        public int CounterOutputCount;
        public Size CounterOutputSize;

        public int AttachmentTombstoneOutputCount;
        public int RevisionTombstoneOutputCount;
        public int DocumentTombstoneOutputCount;
        public int CounterTombstoneOutputCount;

        public int DocumentOutputCount;
        public Size DocumentOutputSize;
    }
}
