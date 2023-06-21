using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class IncomingExternalReplicationHandlerForShard : IncomingReplicationHandler
    {
        private readonly ShardedDocumentDatabase _shardedDatabase;

        public IncomingExternalReplicationHandlerForShard(
            TcpConnectionOptions options,
            ReplicationLatestEtagRequest replicatedLastEtag,
            ReplicationLoader parent, JsonOperationContext.MemoryBuffer bufferToCopy,
            ReplicationLatestEtagRequest.ReplicationType replicationType) : base(options, replicatedLastEtag, parent, bufferToCopy, replicationType)
        {
            _shardedDatabase = ShardedDocumentDatabase.CastToShardedDocumentDatabase(parent.Database);
        }

        protected override DocumentMergedTransactionCommand GetMergeDocumentsCommand(DocumentsOperationContext context,
            DataForReplicationCommand data, long lastDocumentEtag)
        {
            return new MergedDocumentReplicationForShardCommand(_shardedDatabase, data, lastDocumentEtag);
        }

        internal class MergedDocumentReplicationForShardCommand : MergedDocumentReplicationCommand
        {
            private readonly ShardedDocumentDatabase _database;

            public MergedDocumentReplicationForShardCommand(ShardedDocumentDatabase database, DataForReplicationCommand replicationInfo, long lastEtag) : base(replicationInfo, lastEtag)
            {
                _database = database;
            }

            protected override ChangeVector PreProcessItem(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                var changeVector = context.GetChangeVector(item.ChangeVector);
                var result = _database.DocumentsStorage.GetNewChangeVector(context);

                var migratedChangeVector = context.GetChangeVector(changeVector.Version, result.ChangeVector);
                item.ChangeVector = migratedChangeVector.AsString();

                return result.ChangeVector;
            }
        }
    }
}
