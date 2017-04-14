using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Replication
{
    public class IncomingReplicationPerformanceStats : ReplicationPerformanceBase
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

        public IncomingNetworkStats Network { get; set; }

        public List<ReplicationError> Errors { get; set; }
        
        public class IncomingNetworkStats
        {
            public int InputCount { get; set; }

            public int DocumentReadCount { get; set; }
            public int DocumentTombstoneReadCount { get; set; }
            public int AttachmentTombstoneReadCount { get; set; }
            public int AttachmentReadCount { get; set; }
        }
    }
}
