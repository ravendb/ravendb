namespace Raven.Client.Documents.Replication
{
    public class ReplicationPerformance
    {
        public OutgoingStats[] Outgoing { get; set; }

        public IncomingStats[] Incoming { get; set; }

        public class OutgoingStats
        {
            public string Destination { get; set; }

            public OutgoingReplicationPerformanceStats[] Performance { get; set; }
        }

        public class IncomingStats
        {
            public string Source { get; set; }

            public IncomingReplicationPerformanceStats[] Performance { get; set; }
        }
    }
}
