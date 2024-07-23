using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Revisions;

public partial class RevisionsStorage
{
    public bool IsAllowedToDeleteRevisionsManually(string collection)
    {
        var configuration = GetRevisionsConfiguration(collection);
        if (configuration == ConflictConfiguration.Default)
            return false;

        return configuration.AllowDeleteRevisionsManually;
    }

    public Task DeleteRevisionsByChangeVectorManuallyAsync(List<string> cvs, long maxDeletes)
    {
        if (cvs == null || cvs.Count == 0)
            return Task.CompletedTask;

        if (cvs.Count > maxDeletes)
            return Task.FromException(new InvalidOperationException($"You are trying to delete more revisions then the limit: {maxDeletes}"));

        return _database.TxMerger.Enqueue(new DeleteRevisionsByChangeVectorManuallyMergedCommand(cvs));
    }

    private void DeleteRevisionsByChangeVectorManuallyInternal(DocumentsOperationContext context, List<string> cvs)
    {
        var lastModifiedTicks = _database.Time.GetUtcNow().Ticks;

        var table = new Table(RevisionsSchema, context.Transaction.InnerTransaction);

        var writeTables = new Dictionary<string, Table>();

        foreach (var cv in cvs)
        {
            if (string.IsNullOrEmpty(cv))
                throw new ArgumentException("Change Vector is null or empty");

            Document revision;
            using (Slice.From(context.Allocator, cv, out var cvSlice))
            {
                if (table.ReadByKey(cvSlice, out TableValueReader tvr) == false)
                    throw new InvalidOperationException($"Revision with the cv \"{cv}\" doesn't exist");

                revision = TableValueToRevision(context, ref tvr, DocumentFields.ChangeVector | DocumentFields.LowerId);
            }

            using (DocumentIdWorker.GetSliceFromId(context, revision.LowerId, out var lowerId))
            using (GetKeyPrefix(context, lowerId, out var lowerIdPrefix))
            {
                var collectionName = GetCollectionFor(context, lowerIdPrefix);
                if (collectionName == null)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Tried to delete revision {revision.ChangeVector} ({revision.LowerId}) but no collection found.");
                    continue;
                }

                if (context.DocumentDatabase.DocumentsStorage.RevisionsStorage.IsAllowedToDeleteRevisionsManually(collectionName.Name) == false)
                    throw new InvalidOperationException(
                        $"You are trying to delete revisions of '{revision.LowerId}' but it isn't allowed by its revisions configuration.");

                var collectionTable = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);

                DeleteRevisionFromTable(context, collectionTable, writeTables, revision, collectionName, context.GetChangeVector(cv), lastModifiedTicks, revision.Flags);
                IncrementCountOfRevisions(context, lowerIdPrefix, -1);
            }
        }
    }

    internal sealed class DeleteRevisionsByChangeVectorManuallyMergedCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        private readonly List<string> _cvs;

        public DeleteRevisionsByChangeVectorManuallyMergedCommand(List<string> cvs)
        {
            _cvs = cvs;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            context.DocumentDatabase.DocumentsStorage.RevisionsStorage.DeleteRevisionsByChangeVectorManuallyInternal(context, _cvs);
            return 1;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>>
            ToDto(DocumentsOperationContext context)
        {
            throw new NotImplementedException();
        }
    }

    public Task DeleteRevisionsByDocumentIdManuallyAsync(List<string> ids, long maxDeletes)
    {
        if (ids == null || ids.Count == 0)
            return Task.CompletedTask;

        return _database.TxMerger.Enqueue(new DeleteRevisionsByDocumentIdManuallyMergedCommand(ids, maxDeletes));
    }

    private void DeleteRevisionsByDocumentIdManuallyInternal(DocumentsOperationContext context, string id, long maxDeletes, ref long remainingDeletes)
    {
        var result = new DeleteOldRevisionsResult();

        using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
        using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
        {
            var collectionName = GetCollectionFor(context, prefixSlice);
            if (collectionName == null)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Tried to delete revisions for '{id}' but no revisions found.");
                return;
            }

            if (context.DocumentDatabase.DocumentsStorage.RevisionsStorage.IsAllowedToDeleteRevisionsManually(collectionName.Name) == false)
                throw new InvalidOperationException($"You are trying to delete revisions of '{id}' but it isn't allowed by its revisions configuration.");

            var table = EnsureRevisionTableCreated(context.Transaction.InnerTransaction, collectionName);
            var newEtag = _documentsStorage.GenerateNextEtag();
            var changeVector = _documentsStorage.GetNewChangeVector(context, newEtag);

            var lastModifiedTicks = _database.Time.GetUtcNow().Ticks;
            var revisionsToDelete = GetAllRevisions(context, table, prefixSlice, remainingDeletes, skipForceCreated: false, result);
            var revisionsPreviousCount = GetRevisionsCount(context, prefixSlice);
            if (revisionsPreviousCount > remainingDeletes)
                throw new InvalidOperationException($"You are trying to delete more revisions then the limit: {maxDeletes} (stopped on '{id})'.");

            var deleted = DeleteRevisionsInternal(context, table, lowerId, collectionName, changeVector, lastModifiedTicks, revisionsPreviousCount, revisionsToDelete,
                result, tombstoneFlags: DocumentFlags.FromResharding | DocumentFlags.Artificial);

            remainingDeletes -= deleted;
            var hasMore = result.HasMore && result.Remaining > 0;
            if (hasMore)
                throw new InvalidOperationException($"You are trying to delete more revisions then the limit: {maxDeletes} (stopped on '{id}').");

            IncrementCountOfRevisions(context, prefixSlice, -deleted);
        }
    }

    internal sealed class DeleteRevisionsByDocumentIdManuallyMergedCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        private readonly List<string> _ids;

        private readonly long _maxDeletes;

        public DeleteRevisionsByDocumentIdManuallyMergedCommand(List<string> ids, long maxDeletes)
        {
            _ids = ids;
            _maxDeletes = maxDeletes;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            long remainingDeletes = _maxDeletes;

            foreach (var id in _ids)
            {
                context.DocumentDatabase.DocumentsStorage.RevisionsStorage.DeleteRevisionsByDocumentIdManuallyInternal(context, id, _maxDeletes, ref remainingDeletes);
            }

            return 1;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>>
            ToDto(DocumentsOperationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
