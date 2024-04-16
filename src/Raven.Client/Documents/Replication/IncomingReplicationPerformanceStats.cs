using System;

namespace Raven.Client.Documents.Replication
{
    public sealed class IncomingReplicationPerformanceStats : ReplicationPerformanceBase
    {
        public IncomingReplicationPerformanceStats()
        {
            // for deserialization
        }

        public IncomingReplicationPerformanceStats(TimeSpan duration)
            : base(duration)
        {
        }

        public long ReceivedLastEtag { get; set; }
        public string DatabaseChangeVector { get; set; }

        public NetworkStats Network { get; set; }

        public sealed class NetworkStats
        {
            public int InputCount { get; set; }

            public int DocumentReadCount { get; set; }
            public long DocumentReadSizeInBytes { get; set; }

            public int DocumentTombstoneReadCount { get; set; }
            public long DocumentTombstoneReadSizeInBytes { get; set; }

            public int RevisionReadCount { get; set; }
            public long RevisionReadSizeInBytes { get; set; }

            public int RevisionTombstoneReadCount { get; set; }
            public long RevisionTombstoneReadSizeInBytes { get; set; }

            public int AttachmentReadCount { get; set; }
            public long AttachmentReadSizeInBytes { get; set; }

            public int AttachmentStreamReadCount { get; set; }
            public long AttachmentStreamReadSizeInBytes { get; set; }

            public int AttachmentTombstoneReadCount { get; set; }
            public long AttachmentTombstoneReadSizeInBytes { get; set; }

            public int CounterReadCount { get; set; }
            public long CounterReadSizeInBytes { get; set; }

            public int TimeSeriesReadCount { get; set; }
            public long TimeSeriesReadSizeInBytes { get; set; }

            public int TimeSeriesDeletedRangeReadCount { get; set; }
            public long TimeSeriesDeletedRangeReadSizeInBytes { get; set; }
        }
    }
}
