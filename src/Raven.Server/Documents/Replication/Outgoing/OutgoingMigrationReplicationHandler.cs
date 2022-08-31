using System.IO;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Operations;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication.Outgoing
{
    public class OutgoingMigrationReplicationHandler : DatabaseOutgoingReplicationHandler
    {
        private readonly ShardedDocumentDatabase _shardedDatabase;
        public readonly BucketMigrationReplication BucketMigrationNode;
        public string LastChangeVectorInBucket = null;

        public OutgoingMigrationReplicationHandler(ReplicationLoader parent, ShardedDocumentDatabase database, BucketMigrationReplication node, TcpConnectionInfo connectionInfo) : base(parent, database, node, connectionInfo)
        {
            _shardedDatabase = database;
            BucketMigrationNode = node;
            SuccessfulReplication += TryNotifySourceMigrationCompleted;
        }

        private void TryNotifySourceMigrationCompleted(DatabaseOutgoingReplicationHandler handler)
        {
            var current = handler as OutgoingMigrationReplicationHandler;
            var bucket = current.BucketMigrationNode.Bucket;
            var migrationIndex = current.BucketMigrationNode.MigrationIndex;
            var lastSentChangeVector = current.LastChangeVectorInBucket;

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,
                "this is only best effort, this will not solve failovers / writting to a different node / writing after this");
            if (_shardedDatabase.ShardedDocumentsStorage.HaveMoreDocumentsInBucket(bucket, lastSentChangeVector))
                return;

            if (current.Server.Sharding.ManualMigration)
                return;

            var task = current.Server.Sharding.SourceMigrationCompleted(_shardedDatabase.ShardedDatabaseName, bucket, migrationIndex, lastSentChangeVector,
                $"{bucket}@{migrationIndex}/{lastSentChangeVector}");

            task.Wait(_shardedDatabase.DatabaseShutdown);

            var result = task.Result;

            var op = new WaitForIndexNotificationOperation(result.Index);
            _shardedDatabase.DatabaseContext.AllNodesExecutor.ExecuteParallelForAllAsync(op).Wait(_shardedDatabase.DatabaseShutdown);
        }

        public override ReplicationDocumentSenderBase CreateDocumentSender(Stream stream, Logger logger) => 
            new MigrationReplicationDocumentSender(stream, this, logger);

        protected override DynamicJsonValue GetInitialHandshakeRequest()
        {
            var request = base.GetInitialHandshakeRequest();
            request[nameof(ReplicationLatestEtagRequest.MigrationIndex)] = BucketMigrationNode.MigrationIndex;
            return request;
        }

        public override int GetHashCode() => BucketMigrationNode.GetHashCode();

        public override void Dispose()
        {
            SuccessfulReplication -= TryNotifySourceMigrationCompleted;
            base.Dispose();
        }
    }
}
