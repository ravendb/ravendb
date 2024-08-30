using System;
using System.Collections.Generic;
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

        public (bool MoreWork, long Deleted)? Result;

        public DeleteRevisionsByDocumentIdMergedCommand(List<string> ids, DateTime? from, DateTime? to, bool includeForceCreated)
        {
            _ids = ids;
            _from = from;
            _to = to;
            _includeForceCreated = includeForceCreated;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            Result = DeleteRevisions(context);
            return 1;
        }

        private (bool MoreWork, long Deleted)? DeleteRevisions(DocumentsOperationContext context)
        {
            const long maxTotalDeletes = 10_000;

            var revisionsStorage = context.DocumentDatabase.DocumentsStorage.RevisionsStorage;
            var moreWork = false;
            var deleted = 0L;

            for (int i = _ids.Count - 1; i >= 0; i--)
            {
                var id = _ids[i];
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

                    var maxDeletes = revisionsStorage.GetRevisionsConfiguration(collectionName.Name, deleteRevisionsWhenNoCofiguration: true).MaximumRevisionsToDeleteUponDocumentUpdate;
                    if (maxDeletes == null || maxDeletes > maxTotalDeletes - deleted)
                    {
                        maxDeletes = maxTotalDeletes - deleted;
                    }

					var result = revisionsStorage.ForceDeleteAllRevisionsFor(context, lowerId, prefixSlice, collectionName, maxDeletes, ShouldSkipRevision);
                    if (result.MoreWork == false)
                    {
                        _ids.RemoveAt(i);
                    }
                    moreWork |= result.MoreWork;
                    deleted += result.Deleted;

                    if (deleted >= maxTotalDeletes)
                    {
                        if (i != 0)
                            moreWork = true;
                        
                        break;
                    }
                }
            }

            return (moreWork, deleted);
        }

        private bool ShouldSkipRevision(Document revision)
        {
            if (_includeForceCreated == false || _from.HasValue || _to.HasValue)
            {
                return ShouldSkipForceCreated(_includeForceCreated == false, revision.Flags) || IsRevisionInRange(revision, _from, _to) == false;
            }
            return false;
        }

        private static bool IsRevisionInRange(Document revision, DateTime? from, DateTime? to) =>
            (from.HasValue == false || revision.LastModified >= from.Value) && (to.HasValue == false || revision.LastModified <= to.Value);
        

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
