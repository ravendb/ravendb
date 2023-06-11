using System.Linq;
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

                if (ShouldSkip(item) || HasConflicts(context, item))
                    return changeVector.Order;

                var shouldUpdateDatabaseCv = true;
                var current = context.LastDatabaseChangeVector ?? DocumentsStorage.GetDatabaseChangeVector(context);

                if (current.IsNullOrEmpty == false && current.CheckIfDbIdExists(changeVector.Order))
                {
                    // the destination dbId already exists in the database change vector
                    // so we can avoid generating a new database change vector
                    shouldUpdateDatabaseCv = false;
                }

                var etag = _database.DocumentsStorage.GenerateNextEtag();

                if (shouldUpdateDatabaseCv)
                    context.LastDatabaseChangeVector = _database.DocumentsStorage.GetNewChangeVector(context, etag);

                var dbId = _database.DbBase64Id;
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

            private bool ShouldSkip(ReplicationBatchItem item)
            {
                if (item is AttachmentReplicationItem attachment &&
                    AttachmentsStorage.GetAttachmentTypeByKey(attachment.Key) == AttachmentType.Revision)
                    return true;

                if (item is DocumentReplicationItem doc &&
                    (doc.Flags.Contain(DocumentFlags.Revision) ||
                    doc.Flags.Contain(DocumentFlags.DeleteRevision)))
                    return true;

                return false;
            }

            private static bool HasConflicts(DocumentsOperationContext context, ReplicationBatchItem item)
            {
                // we only care about documents 
                if (item is DocumentReplicationItem doc == false)
                    return false;

                return ConflictsStorage.GetConflictStatusForDocument(context, doc.Id, doc.ChangeVector, out _) == ConflictStatus.Conflict;
            }
        }
    }
}
