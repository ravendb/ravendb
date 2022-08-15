using System.Linq;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication.Incoming
{
    public class IncomingMigrationReplicationHandler : IncomingReplicationHandler
    {
        public const string MigrationTag = "MOVE";

        public IncomingMigrationReplicationHandler(TcpConnectionOptions options, ReplicationLatestEtagRequest replicatedLastEtag, ReplicationLoader parent, JsonOperationContext.MemoryBuffer bufferToCopy, ReplicationLatestEtagRequest.ReplicationType replicationType) : base(options, replicatedLastEtag, parent, bufferToCopy, replicationType)
        {

        }

        protected override TransactionOperationsMerger.MergedTransactionCommand GetMergeDocumentsCommand(DataForReplicationCommand data, long lastDocumentEtag)
        {
            return new MergedIncomingMigrationCommand(data, lastDocumentEtag);
        }

        protected override void HandleHeartbeatMessage(DocumentsOperationContext documentsContext, BlittableJsonReaderObject message)
        {
            // do nothing
        }

        internal class MergedIncomingMigrationCommand : MergedDocumentReplicationCommand
        {
            private ShardedDocumentDatabase _shardedDatabase;
            private ShardBucketMigration _movingBucket;

            public MergedIncomingMigrationCommand(DataForReplicationCommand replicationInfo, long lastEtag) : base(replicationInfo, lastEtag)
            {
            }

            protected override ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                var order = context.DocumentDatabase.DocumentsStorage.GetNewChangeVector(context).ChangeVector;
                var changeVector = context.GetChangeVector(item.ChangeVector);
                if (changeVector.Order.Contains(_shardedDatabase.ShardedDatabaseId))
                    return order;

                var migratedChangeVector = context.GetChangeVector(changeVector.Version, order);
                migratedChangeVector = migratedChangeVector.UpdateOrder(MigrationTag, _shardedDatabase.ShardedDatabaseId, _movingBucket.MigrationIndex, context);
                item.ChangeVector = migratedChangeVector.AsString();

                return order;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                var server = context.DocumentDatabase.ServerStore;
                _shardedDatabase = context.DocumentDatabase as ShardedDocumentDatabase;
                
                // TODO better to get this from the ReplicationLatestEtagRequest
                using (server.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var record = server.Cluster.ReadRawDatabaseRecord(ctx, _shardedDatabase.ShardedDatabaseName);
                    _movingBucket = record.Sharding.BucketMigrations.Single(kvp => kvp.Value.Status == MigrationStatus.Moving).Value;
                }
                // TODO: delete current items in the bucket?
                // TODO: handle the incoming properly
                return base.ExecuteCmd(context);
            }

            protected override void SaveSourceEtag(DocumentsOperationContext context)
            {
                // do nothing
            }
        }
    }
}
