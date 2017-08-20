using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Util.RateLimiting;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Queries.Faceted;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Suggestion;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using DeleteDocumentCommand = Raven.Server.Documents.TransactionCommands.DeleteDocumentCommand;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries
{
    public class QueryRunner
    {
        private readonly DocumentDatabase _database;

        private readonly DocumentsOperationContext _documentsContext;

        public QueryRunner(DocumentDatabase database, DocumentsOperationContext documentsContext)
        {
            _database = database;
            _documentsContext = documentsContext;
        }

        public async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, StringValues includes, long? existingResultEtag, OperationCancelToken token)
        {
            DocumentQueryResult result;
            var sw = Stopwatch.StartNew();
            if (query.Metadata.IsDynamic)
            {
                var runner = new DynamicQueryRunner(_database.IndexStore,_database.DocumentsStorage, _documentsContext, _database.Configuration, token);

                result = await runner.Execute(query, existingResultEtag);
                result.DurationInMs = (long)sw.Elapsed.TotalMilliseconds;
                return result;
            }

            var index = GetIndex(query.Metadata.IndexName);
            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag();
                if (etag == existingResultEtag)
                    return DocumentQueryResult.NotModifiedResult;
            }

            result = await index.Query(query, _documentsContext, token);
            result.DurationInMs = (long)sw.Elapsed.TotalMilliseconds;
            return result;
        }

        public async Task ExecuteStreamQuery(IndexQueryServerSide query, HttpResponse response, BlittableJsonTextWriter writer, OperationCancelToken token)
        {
            if (query.Metadata.IsDynamic)
            {
                var runner = new DynamicQueryRunner(_database.IndexStore, _database.DocumentsStorage, _documentsContext, _database.Configuration, token);

                await runner.ExecuteStream(response, writer, query).ConfigureAwait(false);

                return;
            }

            var index = GetIndex(query.Metadata.IndexName);

            await index.StreamQuery(response, writer, query, _documentsContext, token);
        }

        public Task<FacetedQueryResult> ExecuteFacetedQuery(FacetQueryServerSide query, long? facetsEtag, long? existingResultEtag, OperationCancelToken token)
        {
            if (query.FacetSetupDoc != null)
            {
                FacetSetup facetSetup;
                using (_documentsContext.OpenReadTransaction())
                {
                    var facetSetupAsJson = _database.DocumentsStorage.Get(_documentsContext, query.FacetSetupDoc);
                    if (facetSetupAsJson == null)
                        throw new DocumentDoesNotExistException(query.FacetSetupDoc);

                    try
                    {
                        facetSetup = JsonDeserializationServer.FacetSetup(facetSetupAsJson.Data);
                    }
                    catch (Exception e)
                    {
                        throw new DocumentParseException(query.FacetSetupDoc, typeof(FacetSetup), e);
                    }

                    facetsEtag = facetSetupAsJson.Etag;
                }

                query.Facets = facetSetup.Facets;
            }

            return ExecuteFacetedQuery(query, facetsEtag.Value, existingResultEtag, token);
        }

        private async Task<FacetedQueryResult> ExecuteFacetedQuery(FacetQueryServerSide query, long facetsEtag, long? existingResultEtag, OperationCancelToken token)
        {
            if (query.Metadata.IsDynamic)
                throw new InvalidQueryException("Facet query must be executed against static index.", query.Metadata.QueryText, query.QueryParameters);

            var index = GetIndex(query.Metadata.IndexName);
            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag() ^ facetsEtag;
                if (etag == existingResultEtag)
                    return FacetedQueryResult.NotModifiedResult;
            }

            return await index.FacetedQuery(query, facetsEtag, _documentsContext, token);
        }

        public TermsQueryResultServerSide ExecuteGetTermsQuery(string indexName, string field, string fromValue, long? existingResultEtag, int pageSize, DocumentsOperationContext context, OperationCancelToken token)
        {
            var index = GetIndex(indexName);

            var etag = index.GetIndexEtag();
            if (etag == existingResultEtag)
                return TermsQueryResultServerSide.NotModifiedResult;

            return index.GetTerms(field, fromValue, pageSize, context, token);
        }

        public SuggestionQueryResultServerSide ExecuteSuggestionQuery(SuggestionQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            // Check pre-requisites for the query to happen. 

            if (string.IsNullOrWhiteSpace(query.Term))
                throw new InvalidOperationException("Suggestions queries require a term.");

            if (string.IsNullOrWhiteSpace(query.Field))
                throw new InvalidOperationException("Suggestions queries require a field.");

            var sw = Stopwatch.StartNew();

            // Check definition for the index. 

            var index = GetIndex(query.IndexName);
            var indexDefinition = index.GetIndexDefinition();
            if (indexDefinition == null)
                throw new InvalidOperationException($"Could not find specified index '{this}'.");

            if (indexDefinition.Fields.TryGetValue(query.Field, out IndexFieldOptions field) == false)
                throw new InvalidOperationException($"Index '{this}' does not have a field '{query.Field}'.");

            if (field.Suggestions == null)
                throw new InvalidOperationException($"Index '{this}' does not have suggestions configured for field '{query.Field}'.");

            if (field.Suggestions.Value == false)
                throw new InvalidOperationException($"Index '{this}' have suggestions explicitly disabled for field '{query.Field}'.");

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag();
                if (etag == existingResultEtag.Value)
                    return SuggestionQueryResultServerSide.NotModifiedResult;
            }

            context.OpenReadTransaction();

            var result = index.SuggestionsQuery(query, context, token);
            result.DurationInMs = (int)sw.Elapsed.TotalMilliseconds;
            return result;
        }

        public MoreLikeThisQueryResultServerSide ExecuteMoreLikeThisQuery(MoreLikeThisQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            if (string.IsNullOrEmpty(query.DocumentId) && query.MapGroupFields.Count == 0)
                throw new InvalidOperationException("The document id or map group fields are mandatory");

            var sw = Stopwatch.StartNew();
            var index = GetIndex(query.Metadata.IndexName);

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag();
                if (etag == existingResultEtag.Value)
                    return MoreLikeThisQueryResultServerSide.NotModifiedResult;
            }

            context.OpenReadTransaction();

            var result = index.MoreLikeThisQuery(query, context, token);
            result.DurationInMs = (int)sw.Elapsed.TotalMilliseconds;
            return result;
        }

        public async Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, long? existingResultEtag, OperationCancelToken token)
        {
            if (query.Metadata.IsDynamic)
            {
                var runner = new DynamicQueryRunner(_database.IndexStore, _database.DocumentsStorage, _documentsContext, _database.Configuration, token);
                return await runner.ExecuteIndexEntries(query, existingResultEtag);
            }

            var index = GetIndex(query.Metadata.IndexName);

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag();
                if (etag == existingResultEtag)
                    return IndexEntriesQueryResult.NotModifiedResult;
            }

            return index.IndexEntries(query, _documentsContext, token);
        }

        public List<DynamicQueryToIndexMatcher.Explanation> ExplainDynamicIndexSelection(IndexQueryServerSide query)
        {
            if (query.Metadata.IsDynamic == false)
                throw new InvalidOperationException("Explain can only work on dynamic indexes");

            var runner = new DynamicQueryRunner(_database.IndexStore, _database.DocumentsStorage, _documentsContext, _database.Configuration, OperationCancelToken.None);

            return runner.ExplainIndexSelection(query);
        }

        public Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, Action<DeterminateProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(query, options, context, onProgress, (key, retrieveDetails) =>
            {
                var command = new DeleteDocumentCommand(key, null, _database);

                return new BulkOperationCommand<DeleteDocumentCommand>(command, retrieveDetails, x => new BulkOperationResult.DeleteDetails
                {
                    Id = key,
                    Etag = x.DeleteResult?.Etag
                });
            }, token);
        }

        public Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, DocumentsOperationContext context, Action<DeterminateProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(query, options, context, onProgress, (key, retrieveDetails) =>
            {
                var command = _database.Patcher.GetPatchDocumentCommand(context, key, changeVector: null, patch: patch, patchIfMissing: null, skipPatchIfChangeVectorMismatch: false, debugMode: false);

                return new BulkOperationCommand<PatchDocumentCommand>(command, retrieveDetails, x => new BulkOperationResult.PatchDetails
                {
                    Id = key,
                    ChangeVector = x.PatchResult.ChangeVector,
                    Status = x.PatchResult.Status
                });
            }, token);
        }

        private async Task<IOperationResult> ExecuteOperation<T>(IndexQueryServerSide query, QueryOperationOptions options,
            DocumentsOperationContext context, Action<DeterminateProgress> onProgress, Func<string, bool, BulkOperationCommand<T>> func, OperationCancelToken token)
            where T : TransactionOperationsMerger.MergedTransactionCommand
        {
            var index = GetIndex(query.Metadata.IndexName);

            if (index.Type.IsMapReduce())
                throw new InvalidOperationException("Cannot execute bulk operation on Map-Reduce indexes.");

            query = ConvertToOperationQuery(query, options);

            const int batchSize = 1024;

            Queue<string> resultIds;
            try
            {
                var results = await index.Query(query, context, token).ConfigureAwait(false);
                if (options.AllowStale == false && results.IsStale)
                    throw new InvalidOperationException("Cannot perform bulk operation. Query is stale.");

                resultIds = new Queue<string>(results.Results.Count);

                foreach (var document in results.Results)
                {
                    resultIds.Enqueue(document.Id.ToString());
                }
            }
            finally // make sure to close tx if DocumentConflictException is thrown
            {
                context.CloseTransaction();
            }

            var progress = new DeterminateProgress
            {
                Total = resultIds.Count,
                Processed = 0
            };

            onProgress(progress);

            var result = new BulkOperationResult();

            using (var rateGate = options.MaxOpsPerSecond.HasValue ? new RateGate(options.MaxOpsPerSecond.Value, TimeSpan.FromSeconds(1)) : null)
            {
                while (resultIds.Count > 0)
                {
                    var command = new ExecuteRateLimitedOperations<string>(resultIds, id =>
                    {
                        var subCommand = func(id, options.RetrieveDetails);

                        if (options.RetrieveDetails)
                            subCommand.AfterExecute = details => result.Details.Add(details);

                        return subCommand;
                    }, rateGate, token, batchSize);

                    await _database.TxMerger.Enqueue(command);

                    progress.Processed += command.Processed;

                    onProgress(progress);

                    if (command.NeedWait)
                        rateGate?.WaitToProceed();
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
                WaitForNonStaleResultsTimeout = options.StaleTimeout,
                PageSize = int.MaxValue,
                HighlighterPreTags = query.HighlighterPreTags,
                HighlighterPostTags = query.HighlighterPostTags,
                HighlightedFields = query.HighlightedFields,
                HighlighterKeyName = query.HighlighterKeyName,
                QueryParameters = query.QueryParameters
            };
        }

        private Index GetIndex(string indexName)
        {
            var index = _database.IndexStore.GetIndex(indexName);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(indexName);

            return index;
        }

        private class BulkOperationCommand<T> : TransactionOperationsMerger.MergedTransactionCommand where T : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly T _command;
            private readonly bool _retieveDetails;
            private readonly Func<T, IBulkOperationDetails> _getDetails;

            public BulkOperationCommand(T command, bool retieveDetails, Func<T, IBulkOperationDetails> getDetails)
            {
                _command = command;
                _retieveDetails = retieveDetails;
                _getDetails = getDetails;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                var count = _command.Execute(context);

                if (_retieveDetails)
                    AfterExecute?.Invoke(_getDetails(_command));

                return count;
            }

            public Action<IBulkOperationDetails> AfterExecute { private get; set; }
        }
    }
}
