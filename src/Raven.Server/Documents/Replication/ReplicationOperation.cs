namespace Raven.Server.Documents.Replication
{
    public sealed class ReplicationOperation
    {
        private ReplicationOperation()
        {
        }

        public sealed class Incoming
        {
            private Incoming()
            {
            }

            public const string Network = "Network/Read";
            public const string DocumentRead = "Network/DocumentRead";
            public const string AttachmentRead = "Network/AttachmentRead";
            public const string TombstoneRead = "Network/TombstoneRead";

            public const string Storage = "Storage/Write";
        }

        public sealed class Outgoing
        {
            private Outgoing()
            {
            }

            public const string Storage = "Storage/Read";
            public const string DocumentRead = "Storage/DocumentRead";
            public const string AttachmentRead = "Storage/AttachmentRead";
            public const string TombstoneRead = "Storage/TombstoneRead";
            public const string CounterRead = "Storage/CounterRead";
            public const string TimeSeriesRead = "Storage/TimeSeriesRead";


            public const string Network = "Network/Write";
        }
    }
}
