namespace Raven.Client.Replication.Messages
{
    public class ReplicationLatestEtagRequest
    {
        public string SourceDatabaseName { get; set; }

        public string SourceDatabaseId { get; set; }

        public string SourceUrl { get; set; }

        public string SourceMachineName { get; set; }

        public string ResolverVersion { get; set; }

        public string ResolverId { get; set; }
    }
}