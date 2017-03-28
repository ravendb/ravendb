namespace Raven.Server.Documents.Replication
{
    public class ReplicationOperation
    {
        private ReplicationOperation()
        {
        }

        public class Outgoing
        {
            private Outgoing()
            {
            }

            public const string Storage = "Storage";
            public const string DocumentRead = "Storage/DocumentRead";
            public const string AttachmentRead = "Storage/AttachmentRead";
            public const string TombstoneRead = "Storage/TombstoneRead";

            public const string Network = "Network";
            public const string DocumentWrite = "Network/DocumentWrite";
            public const string AttachmentWrite = "Network/AttachmentWrite";
            public const string TombstoneWrite = "Network/TombstoneWrite";
        }
    }
}