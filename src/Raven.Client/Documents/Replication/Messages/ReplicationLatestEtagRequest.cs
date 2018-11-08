using Raven.Client.ServerWide.Commands;

namespace Raven.Client.Documents.Replication.Messages
{
    public class ReplicationLatestEtagRequest
    {
        public string SourceDatabaseName { get; set; }

        public string SourceDatabaseId { get; set; }

        public string SourceUrl { get; set; }

        public string SourceTag { get; set; }

        public string SourceMachineName { get; set; }
    }

    public class ReplicationInitialRequest
    {
        public bool PullReplication { get; set; }

        public string PullReplicationDefinition { get; set; }

        public TcpConnectionInfo Info { get; set; }

        public string Database { get; set; }

        public string DatabaseGroupId { get; set; }
    }
}
