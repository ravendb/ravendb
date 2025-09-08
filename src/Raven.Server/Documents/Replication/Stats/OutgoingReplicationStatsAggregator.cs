using System;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Documents.Replication;
using Raven.Server.Utils.Stats;
using Sparrow;

namespace Raven.Server.Documents.Replication.Stats
{
    public sealed class OutgoingReplicationStatsAggregator : StatsAggregator<OutgoingReplicationRunStats, OutgoingReplicationStatsScope>
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
                LastAcceptedChangeVector = Stats.LastAcceptedChangeVector,
                BatchSizeInBytes = completed ? Stats.LastBatchSize.GetValue(SizeUnit.Bytes) : null,
                Storage = new OutgoingReplicationPerformanceStats.StorageStats
                {
                    InputCount = Stats.InputCount,
                    ArtificialDocumentSkipCount = Stats.ArtificialDocumentSkipCount,
                    SystemDocumentSkipCount = Stats.SystemDocumentSkipCount,
                    ChangeVectorSkipCount = Stats.ChangeVectorSkipCount
                },
                Network = new OutgoingReplicationPerformanceStats.NetworkStats
                {
                    DocumentOutputCount = Stats.DocumentOutputCount,
                    DocumentOutputSizeInBytes = Stats.DocumentOutputSize.GetValue(SizeUnit.Bytes),
                    AttachmentOutputCount = Stats.AttachmentOutputCount,
                    AttachmentOutputSizeInBytes = Stats.AttachmentOutputSize.GetValue(SizeUnit.Bytes),
                    AttachmentStreamOutputCount = Stats.AttachmentStreamOutputCount,
                    AttachmentStreamOutputSizeInBytes = Stats.AttachmentStreamOutputSize.GetValue(SizeUnit.Bytes),
                    RevisionOutputCount = Stats.RevisionOutputCount,
                    RevisionOutputSizeInBytes = Stats.RevisionOutputSize.GetValue(SizeUnit.Bytes),
                    RevisionTombstoneOutputCount = Stats.RevisionTombstoneOutputCount,
                    RevisionTombstoneOutputSizeInBytes = Stats.RevisionTombstoneOutputSize.GetValue(SizeUnit.Bytes),
                    AttachmentTombstoneOutputCount = Stats.AttachmentTombstoneOutputCount,
                    AttachmentTombstoneOutputSizeInBytes = Stats.AttachmentTombstoneOutputSize.GetValue(SizeUnit.Bytes),
                    DocumentTombstoneOutputCount = Stats.DocumentTombstoneOutputCount,
                    DocumentTombstoneOutputSizeInBytes = Stats.DocumentTombstoneOutputSize.GetValue(SizeUnit.Bytes),
                    CounterOutputCount = Stats.CounterOutputCount,
                    CounterOutputSizeInBytes = Stats.CounterOutputSize.GetValue(SizeUnit.Bytes),
                    TimeSeriesSegmentsOutputCount = Stats.TimeSeriesOutputCount,
                    TimeSeriesSegmentsSizeInBytes =  Stats.TimeSeriesOutputSize.GetValue(SizeUnit.Bytes),
                    TimeSeriesDeletedRangeOutputCount = Stats.TimeSeriesDeletedRangeOutputCount,
                    TimeSeriesDeletedRangeOutputSizeInBytes = Stats.TimeSeriesDeletedRangeOutputSize.GetValue(SizeUnit.Bytes)
                },
                Errors = Stats.Errors
            };
        }
    }

    public sealed class OutgoingReplicationStatsScope : StatsScope<OutgoingReplicationRunStats, OutgoingReplicationStatsScope>
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

        public void RecordChangeVectorSkip()
        {
            _stats.ChangeVectorSkipCount++;
        }

        public void RecordAttachmentOutput(long sizeInBytes)
        {
            _stats.AttachmentOutputCount++;
            _stats.AttachmentOutputSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.LastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordAttachmentStreamOutput(long sizeInBytes)
        {
            _stats.AttachmentStreamOutputCount++;
            _stats.AttachmentStreamOutputSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.LastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordAttachmentTombstoneOutput(long sizeInBytes)
        {
            _stats.AttachmentTombstoneOutputCount++;
            _stats.AttachmentTombstoneOutputSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.LastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordRevisionTombstoneOutput(long sizeInBytes)
        {
            _stats.RevisionTombstoneOutputCount++;
            _stats.RevisionTombstoneOutputSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.LastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordDocumentOutput(long sizeInBytes)
        {
            _stats.DocumentOutputCount++;
            _stats.DocumentOutputSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.LastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordDocumentTombstoneOutput(long sizeInBytes)
        {
            _stats.DocumentTombstoneOutputCount++;
            _stats.DocumentTombstoneOutputSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.LastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordRevisionOutput(long sizeInBytes)
        {
            _stats.RevisionOutputCount++;
            _stats.RevisionOutputSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.LastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordTimeSeriesOutput(long sizeInBytes)
        {
            _stats.TimeSeriesOutputCount++;
            _stats.TimeSeriesOutputSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.LastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordCountersOutput(int numberOfCounters, long sizeInBytes)
        {
            _stats.CounterOutputCount += numberOfCounters;
            _stats.CounterOutputSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.LastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordTimeSeriesDeletedRangeOutput(long sizeInBytes)
        {
            _stats.TimeSeriesDeletedRangeOutputCount++;
            _stats.TimeSeriesDeletedRangeOutputSize.Add(sizeInBytes, SizeUnit.Bytes);
            _stats.LastBatchSize.Add(sizeInBytes, SizeUnit.Bytes);
        }

        public void RecordLastEtag(long etag)
        {
            _stats.LastEtag = etag;
        }

        public void RecordLastAcceptedChangeVector(string changeVector)
        {
            _stats.LastAcceptedChangeVector = changeVector;
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

        public long GetTotalOutputItemsCount()
        {
            return _stats.DocumentOutputCount + 
                   _stats.DocumentTombstoneOutputCount +
                   _stats.AttachmentOutputCount + 
                   _stats.AttachmentTombstoneOutputCount +
                   _stats.CounterOutputCount + 
                   _stats.RevisionOutputCount +
                   _stats.TimeSeriesOutputCount + 
                   _stats.TimeSeriesDeletedRangeOutputCount;
        }
    }

    public sealed class OutgoingReplicationRunStats : ReplicationRunStatsBase
    {
        public long LastEtag;
        public Size LastBatchSize;
        public string LastAcceptedChangeVector;

        public int InputCount;

        public int ArtificialDocumentSkipCount;
        public int SystemDocumentSkipCount;
        public int ChangeVectorSkipCount;

        public int DocumentOutputCount;
        public Size DocumentOutputSize;

        public int RevisionOutputCount;
        public Size RevisionOutputSize;

        public int TimeSeriesOutputCount;
        public Size TimeSeriesOutputSize;

        public int AttachmentOutputCount;
        public Size AttachmentOutputSize;

        public int AttachmentStreamOutputCount;
        public Size AttachmentStreamOutputSize;

        public int CounterOutputCount;
        public Size CounterOutputSize;

        public int DocumentTombstoneOutputCount;
        public Size DocumentTombstoneOutputSize;

        public int AttachmentTombstoneOutputCount;
        public Size AttachmentTombstoneOutputSize;

        public int RevisionTombstoneOutputCount;
        public Size RevisionTombstoneOutputSize;

        public int TimeSeriesDeletedRangeOutputCount;
        public Size TimeSeriesDeletedRangeOutputSize;
    }
}
