using System;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Replication;
using Raven.Server.Utils.Stats;
using Sparrow;

namespace Raven.Server.Documents.Replication.Stats
{
    public sealed class IncomingReplicationStatsAggregator : StatsAggregator<IncomingReplicationRunStats, IncomingReplicationStatsScope>
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
                DatabaseChangeVector = Stats.DatabaseChangeVector,
                BatchSizeInBytes = completed ? Stats.ReceivedLastBatchSize.GetValue(SizeUnit.Bytes) : null,
                Network = new IncomingReplicationPerformanceStats.NetworkStats
                {
                    InputCount = Stats.InputCount,
                    DocumentReadCount = Stats.DocumentReadCount,
                    DocumentReadSizeInBytes = Stats.DocumentReadSize.GetValue(SizeUnit.Bytes),
                    AttachmentReadCount = Stats.AttachmentReadCount,
                    AttachmentReadSizeInBytes = Stats.AttachmentReadSize.GetValue(SizeUnit.Bytes),
                    AttachmentStreamReadCount = Stats.AttachmentStreamReadCount,
                    AttachmentStreamReadSizeInBytes = Stats.AttachmentStreamReadSize.GetValue(SizeUnit.Bytes),
                    RevisionReadCount = Stats.RevisionReadCount,
                    RevisionReadSizeInBytes = Stats.RevisionReadSize.GetValue(SizeUnit.Bytes),
                    DocumentTombstoneReadCount = Stats.DocumentTombstoneReadCount,
                    DocumentTombstoneReadSizeInBytes = Stats.DocumentTombstoneReadSize.GetValue(SizeUnit.Bytes),
                    AttachmentTombstoneReadCount = Stats.AttachmentTombstoneReadCount,
                    AttachmentTombstoneReadSizeInBytes = Stats.AttachmentTombstoneReadSize.GetValue(SizeUnit.Bytes),
                    CounterReadCount = Stats.CounterReadCount,
                    CounterReadSizeInBytes = Stats.CounterReadSize.GetValue(SizeUnit.Bytes),
                    RevisionTombstoneReadCount = Stats.RevisionTombstoneReadCount,
                    RevisionTombstoneReadSizeInBytes = Stats.RevisionTombstoneReadSize.GetValue(SizeUnit.Bytes),
                    TimeSeriesReadCount = Stats.TimeSeriesReadCount,
                    TimeSeriesReadSizeInBytes = Stats.TimeSeriesReadSize.GetValue(SizeUnit.Bytes),
                    TimeSeriesDeletedRangeReadCount = Stats.TimeSeriesDeletedRangeReadCount,
                    TimeSeriesDeletedRangeReadSizeInBytes = Stats.TimeSeriesDeletedRangeReadSize.GetValue(SizeUnit.Bytes)
                },
                Errors = Stats.Errors
            };
        }
    }

    public sealed class IncomingReplicationStatsScope : StatsScope<IncomingReplicationRunStats, IncomingReplicationStatsScope>
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

        public void RecordDocumentRead(long sizeInBytes)
        {
            _stats.DocumentReadCount++;
            _stats.DocumentReadSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.ReceivedLastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordDocumentTombstoneRead(long sizeInBytes)
        {
            _stats.DocumentTombstoneReadCount++;
            _stats.DocumentTombstoneReadSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.ReceivedLastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordAttachmentTombstoneRead(long sizeInBytes)
        {
            _stats.AttachmentTombstoneReadCount++;
            _stats.AttachmentTombstoneReadSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.ReceivedLastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordRevisionTombstoneRead(long sizeInBytes)
        {
            _stats.RevisionTombstoneReadCount++;
            _stats.RevisionTombstoneReadSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.ReceivedLastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordRevisionRead(long sizeInBytes)
        {
            _stats.RevisionReadCount++;
            _stats.RevisionReadSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.ReceivedLastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordAttachmentRead(long sizeInBytes)
        {
            _stats.AttachmentReadCount++;
            _stats.AttachmentReadSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.ReceivedLastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordAttachmentStreamRead(long sizeInBytes)
        {
            _stats.AttachmentStreamReadCount++;
            _stats.AttachmentStreamReadSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.ReceivedLastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordCountersRead(int numberOfCounters, long sizeInBytes)
        {
            _stats.CounterReadCount += numberOfCounters;
            _stats.CounterReadSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.ReceivedLastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordTimeSeriesRead(long sizeInBytes)
        {
            _stats.TimeSeriesReadCount++;
            _stats.TimeSeriesReadSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.ReceivedLastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordTimeSeriesDeletedRangeRead(long sizeInBytes)
        {
            _stats.TimeSeriesDeletedRangeReadCount++;
            _stats.TimeSeriesDeletedRangeReadSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.ReceivedLastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordInputAttempt()
        {
            _stats.InputCount++;
        }

        public void RecordLastEtag(long etag)
        {
            _stats.LastEtag = etag;
        }

        public void RecordDatabaseChangeVector(string changeVector)
        {
            _stats.DatabaseChangeVector = changeVector;
        }

        public void AddError(Exception exception)
        {
            _stats.AddError(exception);
        }
    }

    public sealed class IncomingReplicationRunStats : ReplicationRunStatsBase
    {
        public int InputCount;

        public long LastEtag;
        public string DatabaseChangeVector;
        public Size ReceivedLastBatchSize;

        public int DocumentReadCount;
        public Size DocumentReadSize;

        public int DocumentTombstoneReadCount;
        public Size DocumentTombstoneReadSize;

        public int AttachmentTombstoneReadCount;
        public Size AttachmentTombstoneReadSize;

        public int AttachmentReadCount;
        public Size AttachmentReadSize;

        public int AttachmentStreamReadCount;
        public Size AttachmentStreamReadSize;

        public int RevisionTombstoneReadCount;
        public Size RevisionTombstoneReadSize;

        public int RevisionReadCount;
        public Size RevisionReadSize;

        public int CounterReadCount;
        public Size CounterReadSize;

        public int TimeSeriesReadCount;
        public Size TimeSeriesReadSize;

        public int TimeSeriesDeletedRangeReadCount;
        public Size TimeSeriesDeletedRangeReadSize;
    }
}
