using Raven.Client.ServerWide.Commands;

namespace Raven.Client.Documents.Replication.Messages
{
    public sealed class ReplicationLatestEtagRequest
    {
        public string SourceDatabaseName { get; set; }

        public string SourceDatabaseId { get; set; }

        public string SourceDatabaseBase64Id { get; set; }

        public string SourceUrl { get; set; }

        public string SourceTag { get; set; }

        public string SourceMachineName { get; set; }

        public ReplicationType ReplicationsType { get; set; }

        public long MigrationIndex { get; set; }

        public string ShardedDatabaseId { get; set; }

        public enum ReplicationType
        {
            // External here as the default value to handle older servers, which aren't sending this field.
            External,
            Internal,
            Migration,
            Sharded
        }
    }

    public sealed class ReplicationInitialRequest
    {
        public string PullReplicationDefinitionName { get; set; }
        
        public string PullReplicationSinkTaskName { get; set; }

        public TcpConnectionInfo Info { get; set; }

        public string Database { get; set; }

        public string SourceUrl { get; set; }

        public string DatabaseGroupId { get; set; }
    }
}
