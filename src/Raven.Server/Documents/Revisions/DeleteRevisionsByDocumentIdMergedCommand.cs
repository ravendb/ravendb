using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Revisions;

public partial class RevisionsStorage
{
    internal sealed class DeleteRevisionsByDocumentIdMergedCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        private readonly string _id;

        private readonly long _maxDeletes;

        private readonly DateTime? _after, _before;

        public long? Result { get; private set; } // number of deleted revisions

        public DeleteRevisionsByDocumentIdMergedCommand(string ids, long maxDeletes, DateTime? after, DateTime? before)
        {
            _id = ids;
            _maxDeletes = maxDeletes;
            _after = after;
            _before = before;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            Result = DeleteRevisions(context);
            return 1;
        }

        private long DeleteRevisions(DocumentsOperationContext context)
        {
            var revisionsStorage = context.DocumentDatabase.DocumentsStorage.RevisionsStorage;

            using (DocumentIdWorker.GetSliceFromId(context, _id, out Slice lowerId))
            using (revisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefixSlice))
            {
                var collectionName = revisionsStorage.GetCollectionFor(context, prefixSlice);
                if (collectionName == null)
                {
                    if (revisionsStorage._logger.IsInfoEnabled)
                        revisionsStorage._logger.Info($"Tried to delete revisions for '{_id}' but no revisions found.");
                    return 0;
                }

                return revisionsStorage.ForceDeleteAllRevisionsForInternal(context, lowerId, prefixSlice, collectionName, _maxDeletes,
                    shouldSkip: revision =>
                    {
                        if (revisionsStorage.IsAllowedToDeleteRevisionsManually(collectionName.Name, revision.Flags) == false)
                            throw new InvalidOperationException($"You are trying to delete revisions of '{_id}' but it isn't allowed by its revisions configuration.");

                        return IsRevisionInRange(revision, _after, _before) == false;
                    });
            }
        }

        private static bool IsRevisionInRange(Document revision, DateTime? after, DateTime? before)
        {
            return (after.HasValue == false || revision.LastModified > after.Value) &&
                   (before.HasValue == false || revision.LastModified < before.Value);
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new DeleteRevisionsByDocumentIdMergedCommandDto(_id, _maxDeletes, _after, _before);
        }

        private sealed class DeleteRevisionsByDocumentIdMergedCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DeleteRevisionsByDocumentIdMergedCommand>
        {
            private readonly string _id;

            private readonly long _maxDeletes;

            private readonly DateTime? _after, _before;

            public DeleteRevisionsByDocumentIdMergedCommandDto(string ids, long maxDeletes, DateTime? after, DateTime? before)
            {
                _id = ids;
                _maxDeletes = maxDeletes;
                _after = after;
                _before = before;
            }

            public DeleteRevisionsByDocumentIdMergedCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new DeleteRevisionsByDocumentIdMergedCommand(_id, _maxDeletes, _after, _before);
            }
        }
    }
}
