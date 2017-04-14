using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Replication
{
    public class OutgoingReplicationPerformanceStats : ReplicationPerformanceBase
    {
        public OutgoingReplicationPerformanceStats()
        {
            // for deserialization
        }

        public OutgoingReplicationPerformanceStats(TimeSpan duration) 
            : base(duration)
        {
        }

        public long SendLastEtag { get; set; }

        public StorageStats Storage { get; set; }

        public OutgoingNetworkStats Network { get; set; }

        public List<ReplicationError> Errors { get; set; }
        
        public class OutgoingNetworkStats
        {
            public int AttachmentOutputCount { get; set; }
            public long AttachmentOutputSizeInBytes { get; set; }

            public int AttachmentTombstoneOutputCount { get; set; }
            public long AttachmentTombstoneOutputSizeInBytes { get; set; }

            public int DocumentTombstoneOutputCount { get; set; }
            public long DocumentTombstoneOutputSizeInBytes { get; set; }

            public int DocumentOutputCount { get; set; }
            public long DocumentOutputSizeInBytes { get; set; }
        }

        public class StorageStats
        {
            public int InputCount { get; set; }

            public int ArtificialDocumentSkipCount { get; set; }
            public int SystemDocumentSkipCount { get; set; }
            public int DocumentChangeVectorSkipCount { get; set; }
        }
    }

    public abstract class ReplicationPerformanceBase
    {
        protected ReplicationPerformanceBase()
        {
            // for deserialization
        }

        protected ReplicationPerformanceBase(TimeSpan duration)
        {
            DurationInMilliseconds = Math.Round(duration.TotalMilliseconds, 2);
        }

        public int Id { get; set; }

        public DateTime Started { get; set; }

        public double DurationInMilliseconds { get; set; }

        public DateTime? Completed { get; set; }

        public ReplicationPerformanceOperation Details { get; set; }
    }
}
