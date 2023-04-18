using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication.Incoming
{
    public class IncomingMigrationReplicationHandler : IncomingReplicationHandler
    {
        private readonly ReplicationLoader _parent;
        private readonly long _currentMigrationIndex;
        private ShardedDocumentDatabase _shardedDatabase;

        public const string MigrationTag = "MOVE";

        public IncomingMigrationReplicationHandler(TcpConnectionOptions options, ReplicationLatestEtagRequest replicatedLastEtag, ShardReplicationLoader parent,
            JsonOperationContext.MemoryBuffer bufferToCopy, ReplicationLatestEtagRequest.ReplicationType replicationType, long migrationIndex) : base(options, replicatedLastEtag, parent, bufferToCopy, replicationType)
        {
            _parent = parent;
            _currentMigrationIndex = migrationIndex;
            _shardedDatabase = ShardedDocumentDatabase.CastToShardedDocumentDatabase(parent.Database);
        }

        protected override DocumentMergedTransactionCommand GetMergeDocumentsCommand(DocumentsOperationContext context,
            DataForReplicationCommand data, long lastDocumentEtag)
        {
            return new MergedIncomingMigrationCommand(_shardedDatabase, data, lastDocumentEtag, _currentMigrationIndex);
        }

        protected override void HandleHeartbeatMessage(DocumentsOperationContext documentsContext, BlittableJsonReaderObject message)
        {
            // do nothing
        }

        internal class MergedIncomingMigrationCommand : MergedDocumentReplicationCommand
        {
            private readonly ShardedDocumentDatabase _database;
            private readonly long _migrationIndex;

            public MergedIncomingMigrationCommand(ShardedDocumentDatabase database, DataForReplicationCommand replicationInfo, long lastEtag, long migrationIndex) : base(replicationInfo, lastEtag)
            {
                _database = database;
                _migrationIndex = migrationIndex;
            }

            protected override ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                switch (item)
                {
                    case DocumentReplicationItem doc:
                        doc.Flags |= DocumentFlags.FromResharding;
                        break;
                }

                var order = _database.DocumentsStorage.GetNewChangeVector(context).ChangeVector;
                var changeVector = context.GetChangeVector(item.ChangeVector);

                var migratedChangeVector = context.GetChangeVector(changeVector.Version, order);
                migratedChangeVector = migratedChangeVector.UpdateOrder(MigrationTag, _database.ShardedDatabaseId, _migrationIndex, context);
                item.ChangeVector = migratedChangeVector.AsString();

                return order;
            }

            protected override void SetIsIncomingReplication()
            {
                // we want changes to be propagated immediately to the replicas
            }

            protected override void SaveSourceEtag(DocumentsOperationContext context)
            {
                // do nothing
            }
        }
    }
}
