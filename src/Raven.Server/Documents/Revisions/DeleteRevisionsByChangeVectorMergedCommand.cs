using System;
using System.Collections.Generic;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Voron.Data.Tables;
using Voron;

namespace Raven.Server.Documents.Revisions;
public partial class RevisionsStorage
{
    internal sealed class DeleteRevisionsByChangeVectorMergedCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        private readonly string _id;

        private readonly List<string> _cvs;

        private readonly bool _includeForceCreated;

        public long? Result;

        public DeleteRevisionsByChangeVectorMergedCommand(string id, List<string> cvs, bool includeForceCreated)
        {
            _id = id;
            _cvs = cvs;
            _includeForceCreated = includeForceCreated;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            Result = DeleteRevisions(context);
            return 1;
        }

        private long? DeleteRevisions(DocumentsOperationContext context)
        {
            var revisionsStorage = context.DocumentDatabase.DocumentsStorage.RevisionsStorage;
            var deleted = 0L;

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

                var maxDeletes = revisionsStorage.GetRevisionsConfiguration(collectionName.Name, deleteRevisionsWhenNoCofiguration: true)
                    .MaximumRevisionsToDeleteUponDocumentUpdate;

                var result = revisionsStorage.ForceDeleteAllRevisionsForInternal(context, lowerId, prefixSlice, collectionName, maxDeletes, ShouldSkipRevision);
                deleted += result.Deleted;
            }

            return deleted;
        }

        private bool ShouldSkipRevision(Document revision)
        {
            return ShouldSkipForceCreated(_includeForceCreated == false, revision.Flags) || _cvs.Contains(revision.ChangeVector) == false;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            return new DeleteRevisionsByChangeVectorMergedCommandDto(_id, _cvs, _includeForceCreated);
        }

        public sealed class DeleteRevisionsByChangeVectorMergedCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DeleteRevisionsByChangeVectorMergedCommand>
        {
            public readonly string Id;

            public readonly List<string> Cvs;

            public readonly bool IncludeForceCreated;

            public DeleteRevisionsByChangeVectorMergedCommandDto(string id, List<string> cvs, bool includeForceCreated)
            {
                Id = id;
                Cvs = cvs;
                IncludeForceCreated = includeForceCreated;
            }

            public DeleteRevisionsByChangeVectorMergedCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
            {
                return new DeleteRevisionsByChangeVectorMergedCommand(Id, Cvs, IncludeForceCreated);
            }
        }
    }

}
