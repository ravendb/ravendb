using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Queries.Faceted;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries
{
    public class StaticIndexQueryRunner : AbstractQueryRunner
    {
        public StaticIndexQueryRunner(DocumentDatabase database) : base(database)
        {
        }

        public override Task<DocumentQueryResult> ExecuteQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, long? existingResultEtag, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag();
                if (etag == existingResultEtag)
                    return Task.FromResult(DocumentQueryResult.NotModifiedResult);
            }
            
            return index.Query(query, documentsContext, token);
        }

        public override Task ExecuteStreamQuery(IndexQueryServerSide query, DocumentsOperationContext documentsContext, HttpResponse response, IStreamDocumentQueryResultWriter writer, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            return index.StreamQuery(response, writer, query, documentsContext, token);
        }

        public override Task<IndexEntriesQueryResult> ExecuteIndexEntriesQuery(IndexQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag();
                if (etag == existingResultEtag)
                    return Task.FromResult(IndexEntriesQueryResult.NotModifiedResult);
            }

            return Task.FromResult(index.IndexEntries(query, context, token));
        }

        public async Task<FacetedQueryResult> ExecuteFacetedQuery(FacetQueryServerSide query, long? facetsEtag, long? existingResultEtag, DocumentsOperationContext documentsContext, OperationCancelToken token)
        {
            if (query.Metadata.IsDynamic)
                throw new InvalidQueryException("Facet query must be executed against static index.", query.Metadata.QueryText, query.QueryParameters);

            if (query.FacetSetupDoc != null)
            {
                FacetSetup facetSetup;
                using (documentsContext.OpenReadTransaction())
                {
                    var facetSetupAsJson = Database.DocumentsStorage.Get(documentsContext, query.FacetSetupDoc);
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

            var index = GetIndex(query.Metadata.IndexName);
            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag() ^ facetsEtag.Value;
                if (etag == existingResultEtag)
                    return FacetedQueryResult.NotModifiedResult;
            }

            return await index.FacetedQuery(query, facetsEtag.Value, documentsContext, token);
        }

        public override Task<IOperationResult> ExecuteDeleteQuery(IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            return ExecuteDelete(query, index, options, context, onProgress, token);
        }

        public override Task<IOperationResult> ExecutePatchQuery(IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, BlittableJsonReaderObject patchArgs, DocumentsOperationContext context, Action<IOperationProgress> onProgress, OperationCancelToken token)
        {
            var index = GetIndex(query.Metadata.IndexName);

            return ExecutePatch(query, index, options, patch, patchArgs, context, onProgress, token);
        }
    }
}
