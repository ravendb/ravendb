using System;
using System.IO;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.ServerWide.Commands;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication.Outgoing
{
    public sealed class OutgoingMigrationReplicationHandler : DatabaseOutgoingReplicationHandler
    {
        private readonly ShardedDocumentDatabase _shardedDatabase;
        public readonly BucketMigrationReplication BucketMigrationNode;
        public long LastSentEtag;

        public OutgoingMigrationReplicationHandler(ShardReplicationLoader parent, ShardedDocumentDatabase database, BucketMigrationReplication node, TcpConnectionInfo connectionInfo) : base(parent, database, node, connectionInfo)
        {
            _shardedDatabase = database;
            BucketMigrationNode = node;
            SuccessfulReplication += TryNotifySourceMigrationCompleted;
        }

        private void TryNotifySourceMigrationCompleted(DatabaseOutgoingReplicationHandler handler)
        {
            var current = handler as OutgoingMigrationReplicationHandler ??
                          throw new ArgumentException($"Handler must be of type 'OutgoingMigrationReplicationHandler', but is actually {handler.GetType().FullName}");

            var bucket = current.BucketMigrationNode.Bucket;
            var migrationIndex = current.BucketMigrationNode.MigrationIndex;

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,
                "this is only best effort, this will not solve failovers / writting to a different node / writing after this");

            if (_shardedDatabase.ShardedDocumentsStorage.HaveMoreDocumentsInBucketAfter(bucket, current.LastSentEtag, out var merged))
                return;

            if (current.Server.Sharding.ManualMigration)
                return;

            current.Server.Sharding.SourceMigrationCompleted(_shardedDatabase.ShardedDatabaseName, bucket, migrationIndex, merged,
                $"{bucket}@{migrationIndex}/{merged}").Wait(_shardedDatabase.DatabaseShutdown);
        }

        public override ReplicationDocumentSenderBase CreateDocumentSender(Stream stream, RavenLogger logger) => 
            new MigrationReplicationDocumentSender(stream, this, logger);

        protected override DynamicJsonValue GetInitialHandshakeRequest()
        {
            var request = base.GetInitialHandshakeRequest();
            request[nameof(ReplicationLatestEtagRequest.MigrationIndex)] = BucketMigrationNode.MigrationIndex;

            // we generate random but stable guid to allow creating several replication channels from this source to the same destination
            // so we can start a new bucket migration while the previous one is might be still open
            var r = new Random((int)BucketMigrationNode.MigrationIndex);
            Span<byte> guid = stackalloc byte[16];
            r.NextBytes(guid);
            request[nameof(ReplicationLatestEtagRequest.SourceDatabaseId)] = new Guid(guid).ToString();

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
