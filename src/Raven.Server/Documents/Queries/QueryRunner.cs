﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Exceptions;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Util.RateLimiting;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.Documents.TransactionCommands;
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

        public async Task<DocumentQueryResult> ExecuteQuery(string indexName, IndexQueryServerSide query, StringValues includes, long? existingResultEtag, OperationCancelToken token)
        {
            DocumentQueryResult result;
            var sw = Stopwatch.StartNew();
            if (Index.IsDynamicIndex(indexName))
            {
                var runner = new DynamicQueryRunner(_database.IndexStore, _database.TransformerStore, _database.DocumentsStorage, _documentsContext, token);

                result = await runner.Execute(indexName, query, existingResultEtag);
                result.DurationInMs = (long)sw.Elapsed.TotalMilliseconds;
                return result;
            }

            var index = GetIndex(indexName);
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

        public async Task ExecuteStreamQuery(string indexName, IndexQueryServerSide query, HttpResponse response, BlittableJsonTextWriter writer, OperationCancelToken token)
        {
            if (Index.IsDynamicIndex(indexName))
            {
                var runner = new DynamicQueryRunner(_database.IndexStore, _database.TransformerStore, _database.DocumentsStorage, _documentsContext, token);

                await runner.ExecuteStream(response, writer, indexName, query).ConfigureAwait(false);

                return;
            }

            var index = GetIndex(indexName);

            await index.StreamQuery(response, writer, query, _documentsContext, token);
        }

        public Task<FacetedQueryResult> ExecuteFacetedQuery(string indexName, FacetQuery query, long? facetsEtag, long? existingResultEtag, OperationCancelToken token)
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

            return ExecuteFacetedQuery(indexName, query, facetsEtag.Value, existingResultEtag, token);
        }

        private async Task<FacetedQueryResult> ExecuteFacetedQuery(string indexName, FacetQuery query, long facetsEtag, long? existingResultEtag, OperationCancelToken token)
        {
            var index = GetIndex(indexName);
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

        public SuggestionsQueryResultServerSide ExecuteSuggestionsQuery(string indexName, SuggestionsQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {            
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            // Check pre-requisites for the query to happen. 
            
            if (Index.IsDynamicIndex(indexName))
                throw new InvalidOperationException("Cannot get suggestions for dynamic indexes, only static indexes with explicitly defined Suggestions are supported");
                    
            if (string.IsNullOrWhiteSpace(query.Term) == false)
                throw new InvalidOperationException("Suggestions queries require a term.");

            if (string.IsNullOrWhiteSpace(query.Field) == false)
                throw new InvalidOperationException("Suggestions queries require a field.");

            var sw = Stopwatch.StartNew();

            // Check definition for the index. 

            var index = GetIndex(indexName);
            var indexDefinition = index.GetIndexDefinition();
            if (indexDefinition == null)
                throw new InvalidOperationException($"Could not find specified index '{this}'.");

            if ( indexDefinition.Fields.TryGetValue(query.Field, out IndexFieldOptions field) == false)
                throw new InvalidOperationException($"Index '{this}' does not have a field '{query.Field}'.");

            if (field.Suggestions == null)
                throw new InvalidOperationException($"Index '{this}' does not have suggestions configured for field '{query.Field}'.");

            if (field.Suggestions.Value == false)
                throw new InvalidOperationException($"Index '{this}' have suggestions explicitly disabled for field '{query.Field}'.");

            var etag = index.GetIndexEtag();
            if (etag == existingResultEtag)
                return SuggestionsQueryResultServerSide.NotModifiedResult;

            context.OpenReadTransaction();

            var result = index.SuggestionsQuery(query, context, token);
            result.DurationInMs = (int)sw.Elapsed.TotalMilliseconds;
            return result;
        }

        public MoreLikeThisQueryResultServerSide ExecuteMoreLikeThisQuery(string indexName, MoreLikeThisQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            if (string.IsNullOrEmpty(query.DocumentId) && query.MapGroupFields.Count == 0)
                throw new InvalidOperationException("The document id or map group fields are mandatory");

            var sw = Stopwatch.StartNew();
            var index = GetIndex(indexName);

            var etag = index.GetIndexEtag();
            if (etag == existingResultEtag)
                return MoreLikeThisQueryResultServerSide.NotModifiedResult;

            context.OpenReadTransaction();

            var result = index.MoreLikeThisQuery(query, context, token);
            result.DurationInMs = (int)sw.Elapsed.TotalMilliseconds;
            return result;
        }

        public async Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(string indexName, IndexQueryServerSide query, long? existingResultEtag, OperationCancelToken token)
        {
            if (Index.IsDynamicIndex(indexName))
            {
                var runner = new DynamicQueryRunner(_database.IndexStore, _database.TransformerStore, _database.DocumentsStorage, _documentsContext, token);
                return await runner.ExecuteIndexEntries(indexName, query, existingResultEtag);
            }

            var index = GetIndex(indexName);

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag();
                if (etag == existingResultEtag)
                    return IndexEntriesQueryResult.NotModifiedResult;
            }

            return index.IndexEntries(query, _documentsContext, token);
        }

        public List<DynamicQueryToIndexMatcher.Explanation> ExplainDynamicIndexSelection(string indexName, IndexQueryServerSide indexQuery)
        {
            if (Index.IsDynamicIndex(indexName) == false)
                throw new InvalidOperationException("Explain can only work on dynamic indexes");

            var runner = new DynamicQueryRunner(_database.IndexStore, _database.TransformerStore, _database.DocumentsStorage, _documentsContext, OperationCancelToken.None);

            return runner.ExplainIndexSelection(indexName, indexQuery);
        }

        public Task<IOperationResult> ExecuteDeleteQuery(string indexName, IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, Action<DeterminateProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(indexName, query, options, context, onProgress, (key, retrieveDetails) =>
            {
                var command = new DeleteDocumentCommand(key, null, _database);

                return new BulkOperationCommand<DeleteDocumentCommand>(command, retrieveDetails, x => new BulkOperationResult.DeleteDetails
                {
                    Id = key,
                    Etag = x.DeleteResult?.Etag,
                });
            }, token);
        }

        public Task<IOperationResult> ExecutePatchQuery(string indexName, IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, DocumentsOperationContext context, Action<DeterminateProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(indexName, query, options, context, onProgress, (key, retrieveDetails) =>
            {
                var command = _database.Patcher.GetPatchDocumentCommand(context, key, etag: null, patch: patch, patchIfMissing: null, skipPatchIfEtagMismatch: false, debugMode: false);

                return new BulkOperationCommand<PatchDocumentCommand>(command, retrieveDetails, x => new BulkOperationResult.PatchDetails
                {
                    Id = key,
                    Etag = x.PatchResult.Etag,
                    Status = x.PatchResult.Status
                });
            }, token);
        }

        private async Task<IOperationResult> ExecuteOperation<T>(string indexName, IndexQueryServerSide query, QueryOperationOptions options,
            DocumentsOperationContext context, Action<DeterminateProgress> onProgress, Func<string, bool, BulkOperationCommand<T>> func, OperationCancelToken token)
            where T : TransactionOperationsMerger.MergedTransactionCommand
        {
            var index = GetIndex(indexName);

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
            return new IndexQueryServerSide
            {
                Query = query.Query,
                Start = query.Start,
                WaitForNonStaleResultsTimeout = options.StaleTimeout,
                PageSize = int.MaxValue,
                SortedFields = query.SortedFields,
                HighlighterPreTags = query.HighlighterPreTags,
                HighlighterPostTags = query.HighlighterPostTags,
                HighlightedFields = query.HighlightedFields,
                HighlighterKeyName = query.HighlighterKeyName,
                TransformerParameters = query.TransformerParameters,
                Transformer = query.Transformer
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

            public Action<IBulkOperationDetails> AfterExecute { get; set; }
        }
    }
}