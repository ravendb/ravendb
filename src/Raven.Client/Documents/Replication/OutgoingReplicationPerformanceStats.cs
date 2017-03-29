using System;

namespace Raven.Client.Documents.Replication
{
    public class OutgoingReplicationPerformanceStats : ReplicationPerformanceBase<ReplicationPerformanceOperation>
    {
        public OutgoingReplicationPerformanceStats()
        {
            // for deserialization
        }

        public OutgoingReplicationPerformanceStats(TimeSpan duration) 
            : base(duration)
        {
        }

        public int InputCount { get; set; }
        public int ArtificialDocumentSkipCount { get; set; }
        public int SystemDocumentSkipCount { get; set; }
        public int DocumentChangeVectorSkipCount { get; set; }

        public int AttachmentOutputCount { get; set; }
        public long AttachmentOutputSizeInBytes { get; set; }

        public int TombstoneOutputCount { get; set; }
        public long TombstoneOutputSizeInBytes { get; set; }

        public int DocumentOutputCount { get; set; }
        public long DocumentOutputSizeInBytes { get; set; }
    }

    public abstract class ReplicationPerformanceBase<T>
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

        public T Details { get; set; }
    }
}
