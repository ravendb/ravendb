using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Handlers;
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

        public override async Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            if (query.Metadata.HasOrderByRandom == false && existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag(query.Metadata);
                if (etag == existingResultEtag)
                    return DocumentQueryResult.NotModifiedResult;
            }

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await index.Query(query, documentsContext, token);
            }
        }

        public override async Task ExecuteStreamQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response, IStreamQueryResultWriter<Document> writer, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                await index.StreamQuery(response, writer, query, documentsContext, token);
            }
        }

        public override async Task ExecuteStreamIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response, IStreamQueryResultWriter<BlittableJsonReaderObject> writer, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                await index.StreamIndexEntriesQuery(response, writer, query, documentsContext, token);
            }
        }

        public override async Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag(query.Metadata);
                if (etag == existingResultEtag)
                    return IndexEntriesQueryResult.NotModifiedResult;
            }

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await index.IndexEntries(query, context, token);
            }
        }

        public async Task<FacetedQueryResult> ExecuteFacetedQuery(IndexQueryServerSide query, long? existingResultEtag, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            if (query.Metadata.IsDynamic)
                throw new InvalidQueryException("Facet query must be executed against static index.", query.Metadata.QueryText, query.QueryParameters);

            var fq = FacetQuery.Create(documentsContext, query);

            var index = GetIndex(query.Metadata.IndexName);
            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag(query.Metadata) ^ fq.FacetsEtag;
                if (etag == existingResultEtag)
                    return FacetedQueryResult.NotModifiedResult;
            }

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await index.FacetedQuery(fq, documentsContext, token);
            }
        }

        public override async Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await ExecuteDelete(query, index, options, context, onProgress, token);
            }
        }

        public override async Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, BlittableJsonReaderObject patchArgs, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await ExecutePatch(query, index, options, patch, patchArgs, context, onProgress, token);
            }
        }

        public override async Task<SuggestionQueryResult> ExecuteSuggestionQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            using (QueryRunner.MarkQueryAsRunning(index.Name, query, token))
            {
                return await ExecuteSuggestion(query, index, documentsContext, existingResultEtag, token);
            }
        }
    }
}
