using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication;
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

    public async Task<long> DeleteRevisionsByChangeVectorManuallyAsync(List<string> cvs)
    {
        if (cvs == null || cvs.Count == 0)
            return 0;

        var cmd = new DeleteRevisionsByChangeVectorManuallyMergedCommand(cvs);
        await _database.TxMerger.Enqueue(cmd);
        return cmd.Result.HasValue ? cmd.Result.Value : 0;
    }

    private long DeleteRevisionsByChangeVectorManuallyInternal(DocumentsOperationContext context, List<string> cvs)
    {
        var deleted = 0L;

        var lastModifiedTicks = _database.Time.GetUtcNow().Ticks;

        var table = new Table(context.DocumentDatabase.DocumentsStorage.RevisionsStorage.RevisionsSchema, context.Transaction.InnerTransaction);

        var writeTables = new Dictionary<string, Table>();

        foreach (var cv in cvs)
        {
            if (string.IsNullOrEmpty(cv))
                throw new ArgumentException("Change Vector is null or empty");

            Document revision;
            using (Slice.From(context.Allocator, cv, out var cvSlice))
            {
                if (table.ReadByKey(cvSlice, out TableValueReader tvr) == false)
                {
                    continue;
                }

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
                deleted++;
            }
        }

        return deleted;
    }

    internal sealed class DeleteRevisionsByChangeVectorManuallyMergedCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        private readonly List<string> _cvs;

        public long? Result { get; private set; } // deleted revisions

        public DeleteRevisionsByChangeVectorManuallyMergedCommand(List<string> cvs)
        {
            _cvs = cvs;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            Result = context.DocumentDatabase.DocumentsStorage.RevisionsStorage.DeleteRevisionsByChangeVectorManuallyInternal(context, _cvs);
            return 1;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>>
            ToDto(DocumentsOperationContext context)
        {
            throw new NotImplementedException();
        }
    }

    public async Task<long> DeleteRevisionsByDocumentIdManuallyAsync(string id, long maxDeletes, DateTime? after, DateTime? before)
    {
        if (string.IsNullOrEmpty(id))
            return 0;

        var cmd = new DeleteRevisionsByDocumentIdManuallyMergedCommand(id, maxDeletes, after, before);
        await _database.TxMerger.Enqueue(cmd);
        return cmd.Result.HasValue ? cmd.Result.Value : 0;
    }

    private long DeleteRevisionsByDocumentIdManuallyInternal(DocumentsOperationContext context, string id, long maxDeletes, DateTime? after, DateTime? before)
    {
        using (DocumentIdWorker.GetSliceFromId(context, id, out Slice lowerId))
        using (GetKeyPrefix(context, lowerId, out Slice prefixSlice))
        {
            var collectionName = GetCollectionFor(context, prefixSlice);
            if (collectionName == null)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Tried to delete revisions for '{id}' but no revisions found.");
                return 0;
            }

            if (context.DocumentDatabase.DocumentsStorage.RevisionsStorage.IsAllowedToDeleteRevisionsManually(collectionName.Name) == false)
                throw new InvalidOperationException($"You are trying to delete revisions of '{id}' but it isn't allowed by its revisions configuration.");

            return ForceDeleteAllRevisionsForInternal(context, lowerId, prefixSlice, collectionName, maxDeletes, 
                shouldSkip: after.HasValue || before.HasValue ? revision => IsRevisionInRange(revision, after, before) == false : null);
        }
    }

    internal sealed class DeleteRevisionsByDocumentIdManuallyMergedCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        private readonly string _id;

        private readonly long _maxDeletes;

        private readonly DateTime? _after, _before;

        public long? Result { get; private set; } // deleted revisions

        public DeleteRevisionsByDocumentIdManuallyMergedCommand(string ids, long maxDeletes, DateTime? after, DateTime? before)
        {
            _id = ids;
            _maxDeletes = maxDeletes;
            _after = after;
            _before = before;
        }

        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            Result = context.DocumentDatabase.DocumentsStorage.RevisionsStorage.DeleteRevisionsByDocumentIdManuallyInternal(context, _id, _maxDeletes, _after, _before);
            return 1;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>>
            ToDto(DocumentsOperationContext context)
        {
            throw new NotImplementedException();
        }
    }
}
