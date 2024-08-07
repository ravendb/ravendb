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

                foreach (var cv in _cvs)
                {
                    var result = revisionsStorage.ForceDeleteAllRevisionsFor(context, lowerId, prefixSlice, collectionName,
                        (table, _) => GetRevision(context, table, cv));
                    deleted += result.Deleted;
                }
            }

            return deleted;
        }

        private IEnumerable<Document> GetRevision(DocumentsOperationContext context, Table table, string cv)
        {
            Document revision;
            using (Slice.From(context.Allocator, cv, out var cvSlice))
            {
                if (table.ReadByKey(cvSlice, out TableValueReader tvr) == false)
                    yield break;

                revision = TableValueToRevision(context, ref tvr, DocumentFields.ChangeVector | DocumentFields.LowerId | DocumentFields.Id);
            }

            if (revision.Id != _id)
                throw new InvalidOperationException($"Revision with the cv \"{cv}\" doesn't belong to the doc \"{_id}\" but to the doc \"{revision.Id}\"");

            if (ShouldSkipForceCreated(_includeForceCreated == false, revision.Flags))
                yield break;

            yield return revision;
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
