using System;

namespace Raven.Client.Documents.Replication
{
    public class IncomingReplicationPerformanceStats : ReplicationPerformanceBase<ReplicationPerformanceOperation>
    {
        public IncomingReplicationPerformanceStats()
        {
            // for deserialization
        }

        public IncomingReplicationPerformanceStats(TimeSpan duration)
            : base(duration)
        {
        }

        public int DocumentReadCount { get; set; }
        public int TombstoneReadCount { get; set; }
        public int AttachmentReadCount { get; set; }
        public int InputCount { get; set; }
    }
}
