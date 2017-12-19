using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries
{
    public class QueryRunner : AbstractQueryRunner
    {
        private readonly StaticIndexQueryRunner _static;
        private readonly AbstractQueryRunner _dynamic;
        private readonly CollectionQueryRunner _collection;

        public QueryRunner(DocumentDatabase database) : base(database)
        {
            _static = new StaticIndexQueryRunner(database);
            _dynamic =
                database.Configuration.Indexing.DisableQueryOptimizerGeneratedIndexes ? (AbstractQueryRunner)new InvalidQueryRunner(database) : new DynamicQueryRunner(database);
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

        public async Task<FacetedQueryResult> ExecuteFacetedQuery(IndexQueryServerSide query, long? existingResultEtag, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            if (query.Metadata.IsDynamic)
                throw new InvalidQueryException("Facet query must be executed against static index.", query.Metadata.QueryText, query.QueryParameters);

            var sw = Stopwatch.StartNew();

            var result = await _static.ExecuteFacetedQuery(query, existingResultEtag, documentsContext, token);

            result.DurationInMs = (long)sw.Elapsed.TotalMilliseconds;

            return result;
        }

        public TermsQueryResultServerSide ExecuteGetTermsQuery(Index index, string field, string fromValue, long? existingResultEtag, int pageSize, DocumentsOperationContext context, OperationCancelToken token)
        {
            var etag = index.GetIndexEtag();
            if (etag == existingResultEtag)
                return TermsQueryResultServerSide.NotModifiedResult;

            return index.GetTerms(field, fromValue, pageSize, context, token);
        }

        public async Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            if (query.Metadata.IsDynamic)
                throw new InvalidQueryException("Suggestion query must be executed against static index.", query.Metadata.QueryText, query.QueryParameters);

            var sw = Stopwatch.StartNew();

            if (query.Metadata.SelectFields.Length != 1 || query.Metadata.SelectFields[0].IsSuggest == false)
                throw new InvalidQueryException("Suggestion query must have one suggest token in SELECT.", query.Metadata.QueryText, query.QueryParameters);

            var selectField = (SuggestionField)query.Metadata.SelectFields[0];

            var index = GetIndex(query.Metadata.IndexName);

            var indexDefinition = index.GetIndexDefinition();

            if (indexDefinition.Fields.TryGetValue(selectField.Name, out IndexFieldOptions field) == false)
                throw new InvalidOperationException($"Index '{query.Metadata.IndexName}' does not have a field '{selectField.Name}'.");

            if (field.Suggestions == null)
                throw new InvalidOperationException($"Index '{query.Metadata.IndexName}' does not have suggestions configured for field '{selectField.Name}'.");

            if (field.Suggestions.Value == false)
                throw new InvalidOperationException($"Index '{query.Metadata.IndexName}' have suggestions explicitly disabled for field '{selectField.Name}'.");

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag();
                if (etag == existingResultEtag.Value)
                    return SuggestionQueryResult.NotModifiedResult;
            }

            var result = await index.SuggestionQuery(query, context, token);
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

            if (_dynamic is DynamicQueryRunner d)
                return d.ExplainIndexSelection(query);

            throw new NotSupportedException(InvalidQueryRunner.ErrorMessage);
        }
    }
}
