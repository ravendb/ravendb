using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.Suggestions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using FacetQuery = Raven.Server.Documents.Queries.Facets.FacetQuery;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries
{
    public class StaticIndexQueryRunner : AbstractQueryRunner
    {
        public StaticIndexQueryRunner(DocumentDatabase database) : base(database)
        {
        }

        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            queryContext.WithIndex(index);

            if (query.Metadata.HasOrderByRandom == false && existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag(queryContext, query.Metadata);
                if (etag == existingResultEtag)
                    return DocumentQueryResult.NotModifiedResult;
            }

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await index.Query(query, queryContext, token);
            }
        }

        public override async Task ExecuteStreamQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response, IStreamQueryResultWriter<Document> writer, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            queryContext.WithIndex(index);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token, true))
            {
                await index.StreamQuery(response, writer, query, queryContext, token);
            }
        }

        public override async Task ExecuteStreamIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, HttpResponse response, IStreamQueryResultWriter<BlittableJsonReaderObject> writer, bool ignoreLimit, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            queryContext.WithIndex(index);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token, true))
            {
                await index.StreamIndexEntriesQuery(response, writer, query, queryContext, ignoreLimit, token);
            }
        }

        public override async Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, QueryOperationContext queryContext, bool ignoreLimit, long? existingResultEtag, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            queryContext.WithIndex(index);

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag(queryContext, query.Metadata);
                if (etag == existingResultEtag)
                    return IndexEntriesQueryResult.NotModifiedResult;
            }

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await index.IndexEntries(query, queryContext, ignoreLimit, token);
            }
        }

        public async Task<FacetedQueryResult> ExecuteFacetedQuery(IndexQueryServerSide query, long? existingResultEtag, QueryOperationContext queryContext, OperationCancelToken token)
        {
            if (query.Metadata.IsDynamic)
                throw new InvalidQueryException("Facet query must be executed against static index.", query.Metadata.QueryText, query.QueryParameters);

            var fq = FacetQuery.Create(queryContext.Documents, query);

            var index = GetIndex(query.Metadata.IndexName);

            queryContext.WithIndex(index);

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag(queryContext, query.Metadata) ^ fq.FacetsEtag;
                if (etag == existingResultEtag)
                    return FacetedQueryResult.NotModifiedResult;
            }

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await index.FacetedQuery(fq, queryContext, token);
            }
        }

        public override async Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            queryContext.WithIndex(index);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await ExecuteDelete(query, index, options, queryContext, onProgress, token);
            }
        }

        public override async Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, BlittableJsonReaderObject patchArgs, QueryOperationContext queryContext, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            queryContext.WithIndex(index);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await ExecutePatch(query, index, options, patch, patchArgs, queryContext, onProgress, token);
            }
        }

        public override async Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, QueryOperationContext queryContext, long? existingResultEtag, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            queryContext.WithIndex(index);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await ExecuteSuggestion(query, index, queryContext, existingResultEtag, token);
            }
        }
    }
}
