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
        private readonly IEnumerable<string> _ids;

        private readonly DateTime? _after, _before;

        private readonly bool _includeForceCreated;

        public (bool MoreWork, long Deleted)? Result { get; private set; } // has more to delete, number of deleted revisions

        public DeleteRevisionsByDocumentIdMergedCommand(IEnumerable<string> ids, DateTime? after, DateTime? before, bool includeForceCreated)
        {
            _ids = ids;
            _after = after;
            _before = before;
            _includeForceCreated = includeForceCreated;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            var revisionsStorage = context.DocumentDatabase.DocumentsStorage.RevisionsStorage;
            var moreWork = false;
            var deleted = 0L;

            foreach (var id in _ids)
            {
                using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
                using (revisionsStorage.GetKeyPrefix(context, lowerId, out Slice prefixSlice))
                {
                    var collectionName = revisionsStorage.GetCollectionFor(context, prefixSlice);
                    if (collectionName == null)
                    {
                        if (revisionsStorage._logger.IsInfoEnabled)
                            revisionsStorage._logger.Info($"Tried to delete revisions for '{id}' but no revisions found.");
                        continue;
                    }

                    var maxDeletes = revisionsStorage.GetRevisionsConfiguration(collectionName.Name, deleteRevisionsWhenNoCofiguration: true)
                        .MaximumRevisionsToDeleteUponDocumentUpdate;

                    Func<Document, bool> shouldSkip = null;
                    if (_includeForceCreated == false || _after.HasValue || _before.HasValue)
                    {
                        shouldSkip = revision => SkipForceCreated(revision) || IsRevisionInRange(revision, _after, _before) == false;
                    }

                    var result = revisionsStorage.ForceDeleteAllRevisionsForInternal(context, lowerId, prefixSlice, collectionName, maxDeletes, shouldSkip);
                    moreWork |= result.MoreWork;
                    deleted += result.Deleted;
                }
            }

            Result = (moreWork, deleted);
            return 1;

        }

        private bool SkipForceCreated(Document revision)
        {
            return _includeForceCreated == false && revision.Flags.Contain(DocumentFlags.ForceCreated);
        }

        private static bool IsRevisionInRange(Document revision, DateTime? after, DateTime? before)
        {
            return (after.HasValue == false || revision.LastModified > after.Value) &&
                   (before.HasValue == false || revision.LastModified < before.Value);
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new DeleteRevisionsByDocumentIdMergedCommandDto(_ids, _after, _before, _includeForceCreated);
        }

        private sealed class DeleteRevisionsByDocumentIdMergedCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DeleteRevisionsByDocumentIdMergedCommand>
        {
            private IEnumerable<string> _ids;

            private readonly DateTime? _after, _before;

            private readonly bool _includeForceCreated;

            public DeleteRevisionsByDocumentIdMergedCommandDto(IEnumerable<string> ids,  DateTime? after, DateTime? before, bool includeForceCreated)
            {
                _ids = ids;
                _after = after;
                _before = before;
                _includeForceCreated = includeForceCreated;
            }

            public DeleteRevisionsByDocumentIdMergedCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new DeleteRevisionsByDocumentIdMergedCommand(_ids, _after, _before, _includeForceCreated);
            }
        }
    }
}
