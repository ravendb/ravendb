using System.Linq;
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
                var dbId = _database.DbBase64Id;

                if ((changeVector.IsSingle && changeVector.Contains(dbId)) ||
                    changeVector.Order.Contains(dbId) ||
                    changeVector.Version.Contains(dbId))
                {
                    // this item already exists in storage
                    // was sent directly back from replication (in case of bidirectional replication) or changed in the destination
                    // either way, since the dbId exists in the item's change vector we can safely return without updating
                    return changeVector.Order;
                }

                if (HasConflicts(context, item, changeVector))
                    return changeVector.Order;

                var shouldUpdateDatabaseCv = true;
                var current = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);

                if (current.IsNullOrEmpty == false)
                {
                    var dbIdsFromDestCv = ChangeVector.ExtractDbIdsFromChangeVector(changeVector.Order);
                    if (dbIdsFromDestCv != null &&
                        dbIdsFromDestCv.Any(x => current.Order.Contains(x)))
                    {
                        // the destination dbId already exists in the database change vector
                        // so we can avoid generating a new database change vector
                        shouldUpdateDatabaseCv = false;
                    }
                }

                var etag = _database.DocumentsStorage.GenerateNextEtag();

                if (shouldUpdateDatabaseCv)
                    context.LastDatabaseChangeVector = _database.DocumentsStorage.GetNewChangeVector(context, etag);

                if (changeVector.IsSingle)
                {
                    var version = changeVector.Version;
                    var order = changeVector.UpdateOrder(_database.ServerStore.NodeTag, dbId, etag, context);
                    item.ChangeVector = context.GetChangeVector(version, order);
                }
                else
                {
                    item.ChangeVector = changeVector.UpdateOrder(_database.ServerStore.NodeTag, dbId, etag, context);
                }

                return changeVector.Order;
            }

            private static bool HasConflicts(DocumentsOperationContext context, ReplicationBatchItem item, ChangeVector changeVector)
            {
                // we only care about documents 
                if (item is DocumentReplicationItem doc == false ||
                    doc.Flags.Contain(DocumentFlags.Revision) ||
                    doc.Flags.Contain(DocumentFlags.DeleteRevision))
                    return false;

                var result = context.DocumentDatabase.DocumentsStorage.GetDocumentOrTombstone(context, doc.Id);

                // document or tombstone doesn't exist - no conflict 
                if (result.Missing)
                    return false;

                var existingChangeVector = result.Document != null ?
                    context.GetChangeVector(result.Document.ChangeVector) :
                    context.GetChangeVector(result.Tombstone.ChangeVector);

                return ChangeVectorUtils.GetConflictStatus(changeVector, existingChangeVector) == ConflictStatus.Conflict;
            }
        }
    }
}
