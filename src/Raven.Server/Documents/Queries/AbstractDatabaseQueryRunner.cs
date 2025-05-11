using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Server.Documents.Handlers.Processors.Batches;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Voron.Util.RateLimiting;
using Index = Raven.Server.Documents.Indexes.Index;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries;

public abstract class AbstractDatabaseQueryRunner : AbstractQueryRunner
{
    public readonly DocumentDatabase Database;

    protected QueryRunner QueryRunner => Database.QueryRunner;

    protected AbstractDatabaseQueryRunner(DocumentDatabase database)
    {
        Database = database;
    }

    public Index GetIndex(string indexName, bool throwIfNotExists = true)
    {
        var index = Database.IndexStore.GetIndex(indexName);
        if (index == null && throwIfNotExists)
            IndexDoesNotExistException.ThrowFor(indexName);
            
        if (index?.IsPending == true)
            throw new PendingRollingIndexException($"Cannot use index `{indexName}` on node {Database.ServerStore.NodeTag} because a rolling index deployment is still pending on this node.");

        return index;
    }

    public abstract Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token);

    public abstract Task ExecuteStreamQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response,
        IStreamQueryResultWriter<Document> writer, OperationCancelToken token);

    public abstract Task ExecuteStreamIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response,
        IStreamQueryResultWriter<BlittableJsonReaderObject> writer, bool ignoreLimit, OperationCancelToken token);

    public abstract Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, bool ignoreLimit, long? existingResultEtag, OperationCancelToken token);

    public abstract Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token);

    public abstract Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch,
        BlittableJsonReaderObject patchArgs, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token);

    public abstract Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token);

    protected async Task<SuggestionQueryResult> ExecuteSuggestion(
        IndexQueryServerSide query,
        Index index,
        QueryOperationContext queryContext,
        long? existingResultEtag,
        OperationCancelToken token)
    {
        if (query.Metadata.SelectFields.Length == 0)
            throw new InvalidQueryException("Suggestion query must have at least one suggest token in SELECT.", query.Metadata.QueryText, query.QueryParameters);

        var fields = index.Definition.IndexFields;

        foreach (var f in query.Metadata.SelectFields)
        {
            if (f.IsSuggest == false)
                throw new InvalidQueryException("Suggestion query must have only suggest tokens in SELECT.", query.Metadata.QueryText, query.QueryParameters);

            var selectField = (SuggestionField)f;

            if (fields.TryGetValue(selectField.Name, out var field) == false)
                throw new InvalidOperationException($"Index '{index.Name}' does not have a field '{selectField.Name}'.");

            if (field.HasSuggestions == false)
                throw new InvalidOperationException($"Index '{index.Name}' does not have suggestions configured for field '{selectField.Name}'.");
        }

        if (existingResultEtag.HasValue)
        {
            var etag = index.GetIndexEtag(queryContext, query.Metadata);
            if (etag == existingResultEtag.Value)
                return SuggestionQueryResult.NotModifiedResult;
        }

        return await index.SuggestionQuery(query, queryContext, token).ConfigureAwait(false);
    }

    protected Task<IOperationResult> ExecuteDelete(IndexQueryServerSide query, Index index, QueryOperationOptions options, QueryOperationContext queryContext,
        Action<DeterminateProgress> onProgress, OperationCancelToken token)
    {
        return ExecuteOperation(query, index, options, queryContext, onProgress, (key) =>
        {
            var command = new DeleteDocumentCommand(key, null, Database);

            return new BulkOperationCommand<DeleteDocumentCommand>(command, x =>
                    new BulkOperationResult.DeleteDetails { Id = key, Etag = x.DeleteResult?.Etag, Collection = x.DeleteResult?.Collection.Name}, afterExecuted: null);
        }, token);
    }

    protected Task<IOperationResult> ExecutePatch(IndexQueryServerSide query, Index index, QueryOperationOptions options, PatchRequest patch,
        BlittableJsonReaderObject patchArgs, QueryOperationContext queryContext, Action<DeterminateProgress> onProgress, OperationCancelToken token)
    {
        return ExecuteOperation(query, index, options, queryContext, onProgress,
            (key) =>
            {
                var command = new PatchDocumentCommand(queryContext.Documents, key,
                    expectedChangeVector: null,
                    skipPatchIfChangeVectorMismatch: false,
                    patch: (patch, patchArgs),
                    patchIfMissing: (null, null),
                    identityPartsSeparator: Database.IdentityPartsSeparator,
                    createIfMissing: null,
                    debugMode: false,
                    isTest: false,
                    collectResultsNeeded: true,
                    returnDocument: false,
                    ignoreMaxStepsForScript: options.IgnoreMaxStepsForScript);

                return new BulkOperationCommand<PatchDocumentCommand>(command,
                    x => new BulkOperationResult.PatchDetails { Id = key, ChangeVector = x.PatchResult.ChangeVector, Etag = x.PatchResult.Etag, Status = x.PatchResult.Status, Collection = x.PatchResult.Collection },
                    c => c.PatchResult?.Dispose());
            }, token); 
    }

    private async Task<IOperationResult> ExecuteOperation<T>(
        IndexQueryServerSide query,
        Index index,
        QueryOperationOptions options,
        QueryOperationContext queryContext,
        Action<DeterminateProgress> onProgress,
        Func<string, BulkOperationCommand<T>> createCommandForId,
        OperationCancelToken token)
        where T : DocumentMergedTransactionCommand
    {
        if (index.Type.IsMapReduce())
            throw new InvalidOperationException("Cannot execute bulk operation on Map-Reduce indexes.");

        query = ConvertToOperationQuery(query, options);

        const int batchSize = 1024;
        Queue<string> resultIds;

        var progress = new DeterminateProgress
        {
            Total = 0,
            Processed = 0
        };

        onProgress(progress);

        try
        {
            var results = await index.IdQuery(query, queryContext, progress, onProgress, token).ConfigureAwait(false);
            if (options.AllowStale == false && results.IsStale)
                throw new InvalidOperationException("Cannot perform bulk operation. Index is stale.");

            resultIds = results.DocumentIds;
        }
        finally // make sure to close tx if DocumentConflictException is thrown
        {
            queryContext.CloseTransaction();
        }

        onProgress(progress);

        var result = new BulkOperationResult();
        var information = new AdditionalPatchInformation(options, result, Database.DbBase64Id);

        using (var rateGate = options.MaxOpsPerSecond.HasValue ? new RateGate(options.MaxOpsPerSecond.Value, TimeSpan.FromSeconds(1)) : null)
        {
            while (resultIds.Count > 0)
            {
                var command = new ExecuteRateLimitedOperations<string>(resultIds, id =>
                    {
                        var subCommand = createCommandForId(id);
                        if (subCommand == null)
                            return null;

                        subCommand.RetrieveDetails = information.RetrieveDetails;

                        return subCommand;
                    }, rateGate, token,
                    batchSize: batchSize);

                await Database.TxMerger.Enqueue(command).ConfigureAwait(false);
                progress.Processed += command.Processed;
                onProgress(progress);

                if (command.NeedWait)
                    rateGate?.WaitToProceed();
            }

            if (options.IndexPatchOptions != null)
            {
                await BatchHandlerProcessorForBulkDocs.WaitForIndexesAsync(Database, options.IndexPatchOptions.WaitForIndexesTimeout,
                    options.IndexPatchOptions.WaitForSpecificIndexes, throwOnTimeout: options.IndexPatchOptions.ThrowOnTimeoutInWaitForIndexes, information.LastEtag,
                    lastTombstoneEtag: 0, information.Collections, token.Token);
            }
        }

        result.Total = progress.Total;
        return result;
    }

    private static IndexQueryServerSide ConvertToOperationQuery(IndexQueryServerSide query, QueryOperationOptions options)
    {
        return new IndexQueryServerSide(query.Metadata)
        {
            Query = query.Query,
            Start = query.Start,
            WaitForNonStaleResultsTimeout = options.StaleTimeout ?? query.WaitForNonStaleResultsTimeout,
            PageSize = query.PageSize,
            QueryParameters = query.QueryParameters,
            DocumentFields = DocumentFields.Id
        };
    }
}
