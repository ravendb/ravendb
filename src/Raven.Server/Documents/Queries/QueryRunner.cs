using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Client.Util.RateLimiting;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Queries.Faceted;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Suggestion;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using DeleteDocumentCommand = Raven.Server.Documents.TransactionCommands.DeleteDocumentCommand;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries
{
    public class QueryRunner : AbstractQueryRunner
    {
        private readonly StaticIndexQueryRunner _static;
        private readonly DynamicQueryRunner _dynamic;
        private readonly CollectionQueryRunner _collection;

        public QueryRunner(DocumentDatabase database) : base(database)
        {
            _static = new StaticIndexQueryRunner(database);
            _dynamic = new DynamicQueryRunner(database);
            _collection = new CollectionQueryRunner(database);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private AbstractQueryRunner GetRunner(IndexQueryServerSide query)
        {
            if (query.Metadata.IsDynamic)
            {
                if (query.Metadata.IsCollectionQuery == false)
                    return _dynamic;

                return _collection;
            }

            return _static;
        }

        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            var sw = Stopwatch.StartNew();

            var result = await GetRunner(query).ExecuteQuery(query, documentsContext, existingResultEtag, token);

            result.DurationInMs = (long)sw.Elapsed.TotalMilliseconds;

            return result;
        }

        public override Task ExecuteStreamQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response, IStreamDocumentQueryResultWriter writer, OperationCancelToken token)
        {
            return GetRunner(query).ExecuteStreamQuery(query, documentsContext, response, writer, token);
        }

        public Task<FacetedQueryResult> ExecuteFacetedQuery(FacetQueryServerSide query, long? facetsEtag, long? existingResultEtag, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            if (query.Metadata.IsDynamic)
                throw new InvalidQueryException("Facet query must be executed against static index.", query.Metadata.QueryText, query.QueryParameters);

            return _static.ExecuteFacetedQuery(query, facetsEtag, existingResultEtag, documentsContext, token);
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

        public override Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            return GetRunner(query).ExecuteIndexEntriesQuery(query, context, existingResultEtag, token);
        }

        public override Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return GetRunner(query).ExecuteDeleteQuery(query, options, context, onProgress, token);
        }

        public override Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, BlittableJsonReaderObject patchArgs, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            return GetRunner(query).ExecutePatchQuery(query, options, patch, patchArgs, context, onProgress, token);
        }

        public List<DynamicQueryToIndexMatcher.Explanation> ExplainDynamicIndexSelection(IndexQueryServerSide query)
        {
            if (query.Metadata.IsDynamic == false)
                throw new InvalidOperationException("Explain can only work on dynamic indexes");

            return _dynamic.ExplainIndexSelection(query);
        }

    }
}
