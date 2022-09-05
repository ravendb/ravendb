using System;
using Raven.Client.Documents.Replication.Messages;
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
        private readonly long _currentMigrationIndex;

        public const string MigrationTag = "MOVE";

        public IncomingMigrationReplicationHandler(TcpConnectionOptions options, ReplicationLatestEtagRequest replicatedLastEtag, ReplicationLoader parent,
            JsonOperationContext.MemoryBuffer bufferToCopy, ReplicationLatestEtagRequest.ReplicationType replicationType, long migrationIndex) : base(options, replicatedLastEtag, parent, bufferToCopy, replicationType)
        {
            _currentMigrationIndex = migrationIndex;
        }

        protected override TransactionOperationsMerger.MergedTransactionCommand GetMergeDocumentsCommand(DataForReplicationCommand data, long lastDocumentEtag)
        {
            return new MergedIncomingMigrationCommand(data, lastDocumentEtag, _currentMigrationIndex);
        }

        protected override void HandleHeartbeatMessage(DocumentsOperationContext documentsContext, BlittableJsonReaderObject message)
        {
            // do nothing
        }

        internal class MergedIncomingMigrationCommand : MergedDocumentReplicationCommand
        {
            private readonly long _migrationIndex;
            private ShardedDocumentDatabase _shardedDatabase;

            public MergedIncomingMigrationCommand(DataForReplicationCommand replicationInfo, long lastEtag, long migrationIndex) : base(replicationInfo, lastEtag)
            {
                _migrationIndex = migrationIndex;
            }

            protected override ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                var order = context.DocumentDatabase.DocumentsStorage.GetNewChangeVector(context).ChangeVector;
                var changeVector = context.GetChangeVector(item.ChangeVector);
                if (changeVector.Order.Contains(_shardedDatabase.ShardedDatabaseId))
                    return order;

                var migratedChangeVector = context.GetChangeVector(changeVector.Version, order);
                migratedChangeVector = migratedChangeVector.UpdateOrder(MigrationTag, _shardedDatabase.ShardedDatabaseId, _migrationIndex, context);
                item.ChangeVector = migratedChangeVector.AsString();

                return order;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                _shardedDatabase = ShardedDocumentDatabase.CastToShardedDocumentDatabase(context.DocumentDatabase);
               
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
