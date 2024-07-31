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
        private readonly List<string> _ids;

        private readonly DateTime? _from, _to;

        private readonly bool _includeForceCreated;

        public (bool MoreWork, long Deleted)? Result { get; private set; } // has more to delete, number of deleted revisions

        public DeleteRevisionsByDocumentIdMergedCommand(List<string> ids, DateTime? from, DateTime? to, bool includeForceCreated)
        {
            _ids = ids;
            _from = from;
            _to = to;
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
                    if (_includeForceCreated == false || _from.HasValue || _to.HasValue)
                    {
                        shouldSkip = ShouldSkipRevision;
                    }

                    var result = revisionsStorage.ForceDeleteAllRevisionsForInternal(context, lowerId, prefixSlice, collectionName, maxDeletes, shouldSkip);
                    moreWork |= result.MoreWork;
                    deleted += result.Deleted;
                }
            }

            Result = (moreWork, deleted);
            return 1;

        }

        private bool ShouldSkipRevision(Document revision)
        {
            // here you can use local vars from the outer method
            if (_includeForceCreated == false || _from.HasValue || _to.HasValue)
            {
                return SkipForceCreated(revision) || IsRevisionInRange(revision, _from, _to) == false;
            }
            return false;
        }

        private bool SkipForceCreated(Document revision)
        {
            return _includeForceCreated == false && revision.Flags.Contain(DocumentFlags.ForceCreated);
        }

        private static bool IsRevisionInRange(Document revision, DateTime? from, DateTime? to)
        {
            return (from.HasValue == false || revision.LastModified >= from.Value) &&
                   (to.HasValue == false || revision.LastModified <= to.Value);
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new DeleteRevisionsByDocumentIdMergedCommandDto(_ids, _from, _to, _includeForceCreated);
        }

        public sealed class DeleteRevisionsByDocumentIdMergedCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DeleteRevisionsByDocumentIdMergedCommand>
        {
            public List<string> Ids;

            public readonly DateTime? From, To;

            public readonly bool IncludeForceCreated;

            public DeleteRevisionsByDocumentIdMergedCommandDto(List<string> ids,  DateTime? from, DateTime? to, bool includeForceCreated)
            {
                Ids = ids;
                From = from;
                To = to;
                IncludeForceCreated = includeForceCreated;
            }

            public DeleteRevisionsByDocumentIdMergedCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new DeleteRevisionsByDocumentIdMergedCommand(Ids, From, To, IncludeForceCreated);
            }
        }
    }
}
