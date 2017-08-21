using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Faceted;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Documents.Queries.Suggestion;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Handlers
{
    public class QueriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/queries", "POST", AuthorizationStatus.ValidUser)]
        public async Task Post()
        {
            using (TrackRequestTime())
            using (var token = CreateTimeLimitedOperationToken())
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var debug = GetStringQueryString("debug", required: false);
                if (string.IsNullOrWhiteSpace(debug) == false)
                {
                    await Debug(context, debug, token, HttpMethod.Post);
                    return;
                }

                var operation = GetStringQueryString("op", required: false);
                if (string.Equals(operation, "morelikethis", StringComparison.OrdinalIgnoreCase))
                {
                    MoreLikeThis(context, token, HttpMethod.Post);
                    return;
                }

                if (string.Equals(operation, "facets", StringComparison.OrdinalIgnoreCase))
                {
                    await FacetedQuery(context, token, HttpMethod.Post).ConfigureAwait(false);
                    return;
                }

                if (string.Equals(operation, "suggest", StringComparison.OrdinalIgnoreCase))
                {
                    Suggest(context, token, HttpMethod.Post);
                    return;
                }

                await Query(context, token, HttpMethod.Post).ConfigureAwait(false);
            }
        }

        [RavenAction("/databases/*/queries", "GET", AuthorizationStatus.ValidUser)]
        public async Task Get()
        {
            using (TrackRequestTime())
            using (var token = CreateTimeLimitedOperationToken())
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var debug = GetStringQueryString("debug", required: false);
                if (string.IsNullOrWhiteSpace(debug) == false)
                {
                    await Debug(context, debug, token, HttpMethod.Get);
                    return;
                }

                var operation = GetStringQueryString("op", required: false);
                if (string.Equals(operation, "morelikethis", StringComparison.OrdinalIgnoreCase))
                {
                    MoreLikeThis(context, token, HttpMethod.Get);
                    return;
                }

                if (string.Equals(operation, "facets", StringComparison.OrdinalIgnoreCase))
                {
                    await FacetedQuery(context, token, HttpMethod.Get).ConfigureAwait(false);
                    return;
                }

                if (string.Equals(operation, "suggest", StringComparison.OrdinalIgnoreCase))
                {
                    Suggest(context, token, HttpMethod.Get);
                    return;

                }

                await Query(context, token, HttpMethod.Get).ConfigureAwait(false);
            }
        }

        private async Task FacetedQuery(DocumentsOperationContext context, OperationCancelToken token, HttpMethod method)
        {
            var query = await GetFacetQuery(context, method);
            if (query.FacetQuery.FacetSetupDoc == null && (query.FacetQuery.Facets == null || query.FacetQuery.Facets.Count == 0))
                throw new InvalidOperationException("One of the required parameters (facetDoc or facets) was not specified.");

            var existingResultEtag = GetLongFromHeaders("If-None-Match");

            var runner = new QueryRunner(Database, context);

            var result = await runner.ExecuteFacetedQuery(query.FacetQuery, query.FacetsEtag, existingResultEtag, token);

            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteFacetedQueryResult(context, result);
            }

            Database.QueryMetadataCache.MaybeAddToCache(query.FacetQuery.Metadata, result.IndexName);
        }

        private async Task Query(DocumentsOperationContext context, OperationCancelToken token, HttpMethod method)
        {
            var indexQuery = await GetIndexQuery(context, method);

            var existingResultEtag = GetLongFromHeaders("If-None-Match");
            var includes = GetStringValuesQueryString("include", required: false);
            var metadataOnly = GetBoolValueQueryString("metadata-only", required: false) ?? false;

            var runner = new QueryRunner(Database, context);

            DocumentQueryResult result;
            try
            {
                result = await runner.ExecuteQuery(indexQuery, includes, existingResultEtag, token).ConfigureAwait(false);
            }
            catch (IndexDoesNotExistException)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;
            }

            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

            int numberOfResults;
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteDocumentQueryResult(context, result, metadataOnly, out numberOfResults);
            }

            Database.QueryMetadataCache.MaybeAddToCache(indexQuery.Metadata, result.IndexName);
            AddPagingPerformanceHint(PagingOperationType.Queries, $"{nameof(Query)} ({indexQuery.Metadata.IndexName})", HttpContext, numberOfResults, indexQuery.PageSize, TimeSpan.FromMilliseconds(result.DurationInMs));
        }

        private async Task<IndexQueryServerSide> GetIndexQuery(JsonOperationContext context, HttpMethod method)
        {
            if (method == HttpMethod.Get)
                return IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(), context);

            var json = await context.ReadForMemoryAsync(RequestBodyStream(), "index/query");

            return IndexQueryServerSide.Create(json, context, Database.QueryMetadataCache);
        }

        private MoreLikeThisQueryServerSide GetMoreLikeThisQuery(JsonOperationContext context, HttpMethod method)
        {
            if (method == HttpMethod.Get)
            {
                //MoreLikeThisQueryServerSide.Create(HttpContext, GetPageSize(), context);
                throw new NotImplementedException();
            }

            var json = context.ReadForMemory(RequestBodyStream(), "morelikethis/query");

            return MoreLikeThisQueryServerSide.Create(json);
        }

        private async Task<(FacetQueryServerSide FacetQuery, long? FacetsEtag)> GetFacetQuery(JsonOperationContext context, HttpMethod method)
        {
            if (method == HttpMethod.Get)
            {
                return await FacetQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(), context);
            }

            var json = context.ReadForMemory(RequestBodyStream(), "facet/query");

            // read from cache here

            return FacetQueryServerSide.Create(json, context, Database.QueryMetadataCache);
        }

        private SuggestionQueryServerSide GetSuggestionQuery(JsonOperationContext context, HttpMethod method)
        {
            if (method == HttpMethod.Get)
            {
                //MoreLikeThisQueryServerSide.Create(HttpContext, GetPageSize(), context);
                throw new NotImplementedException();
            }

            var indexQueryJson = context.ReadForMemory(RequestBodyStream(), "suggestion/query");

            // read from cache here

            return SuggestionQueryServerSide.Create(indexQueryJson);
        }

        private void Suggest(DocumentsOperationContext context, OperationCancelToken token, HttpMethod method)
        {
            var existingResultEtag = GetLongFromHeaders("If-None-Match");

            var query = GetSuggestionQuery(context, method);
            var runner = new QueryRunner(Database, context);

            var result = runner.ExecuteSuggestionQuery(query, context, existingResultEtag, token);
            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteSuggestionQueryResult(context, result);
            }

            AddPagingPerformanceHint(PagingOperationType.Queries, $"{nameof(Suggest)} ({query.IndexName})", HttpContext, result.Suggestions.Length, query.PageSize, TimeSpan.FromMilliseconds(result.DurationInMs));
        }

        private void MoreLikeThis(DocumentsOperationContext context, OperationCancelToken token, HttpMethod method)
        {
            var existingResultEtag = GetLongFromHeaders("If-None-Match");

            var query = GetMoreLikeThisQuery(context, method);
            var runner = new QueryRunner(Database, context);

            var result = runner.ExecuteMoreLikeThisQuery(query, context, existingResultEtag, token);

            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

            int numberOfResults;
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteMoreLikeThisQueryResult(context, result, out numberOfResults);
            }

            AddPagingPerformanceHint(PagingOperationType.Queries, $"{nameof(MoreLikeThis)} ({query.Metadata.IndexName})", HttpContext, numberOfResults, query.PageSize, TimeSpan.FromMilliseconds(result.DurationInMs));
        }

        private async Task Explain(DocumentsOperationContext context, HttpMethod method)
        {
            var indexQuery = await GetIndexQuery(context, method);
            var runner = new QueryRunner(Database, context);

            var explanations = runner.ExplainDynamicIndexSelection(indexQuery);

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WriteArray(context, "Results", explanations, (w, c, explanation) =>
                {
                    w.WriteExplanation(context, explanation);
                });
                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/queries", "DELETE", AuthorizationStatus.ValidUser)]
        public Task Delete()
        {
            var returnContextToPool = ContextPool.AllocateOperationContext(out DocumentsOperationContext context); // we don't dispose this as operation is async

            var reader = context.Read(RequestBodyStream(), "queries/delete");
            var query = IndexQueryServerSide.Create(reader, context, Database.QueryMetadataCache);

            if (query.Metadata.IsDynamic == false)
            {
                ExecuteQueryOperation(query.Metadata,
                    (runner, options, onProgress, token) => runner.Query.ExecuteDeleteQuery(query, options.Query, context, onProgress, token),
                    context, returnContextToPool, Operations.Operations.OperationType.DeleteByIndex);
            }
            else
            {
                EnsureQueryHasOnlyFromClause(query.Metadata.Query, query.Metadata.CollectionName);

                ExecuteQueryOperation(query.Metadata,
                    (runner, options, onProgress, token) => runner.Collection.ExecuteDelete(query.Metadata.CollectionName, options.Collection, onProgress, token),
                    context, returnContextToPool, Operations.Operations.OperationType.DeleteByCollection);
            }

            return Task.CompletedTask;

        }

        [RavenAction("/databases/*/queries", "PATCH", AuthorizationStatus.ValidUser)]
        public Task Patch()
        {
            var returnContextToPool = ContextPool.AllocateOperationContext(out DocumentsOperationContext context); // we don't dispose this as operation is async

            var reader = context.Read(RequestBodyStream(), "queries/patch");
            if (reader == null)
                throw new BadRequestException("Missing JSON content.");
            if (reader.TryGet("Patch", out BlittableJsonReaderObject patchJson) == false || patchJson == null)
                throw new BadRequestException("Missing 'Patch' property.");
            if (reader.TryGet("Query", out BlittableJsonReaderObject queryJson) == false || queryJson == null)
                throw new BadRequestException("Missing 'Query' property.");

            var patch = PatchRequest.Parse(patchJson, out var patchArgs);
            var query = IndexQueryServerSide.Create(queryJson, context, Database.QueryMetadataCache);

            if (query.Metadata.IsDynamic == false)
            {
                ExecuteQueryOperation(query.Metadata,
                    (runner, options, onProgress, token) => runner.Query.ExecutePatchQuery(
                        query, options.Query, patch, patchArgs, context, onProgress, token),
                context, returnContextToPool, Operations.Operations.OperationType.UpdateByIndex);
            }
            else
            {
                EnsureQueryHasOnlyFromClause(query.Metadata.Query, query.Metadata.CollectionName);

                ExecuteQueryOperation(query.Metadata,
                    (runner, options, onProgress, token) => runner.Collection.ExecutePatch(query.Metadata.CollectionName, options.Collection, patch, onProgress, token),
                    context, returnContextToPool, Operations.Operations.OperationType.UpdateByCollection);
            }

            return Task.CompletedTask;
        }

        private void EnsureQueryHasOnlyFromClause(Query query, string collection)
        {
            if (query.Where != null || query.Select != null || query.OrderBy != null || query.GroupBy != null || query.With != null)
                throw new BadRequestException($"Patch and delete documents by a dynamic query is supported only for queries having just FROM clause, e.g. 'FROM {collection}'. If you need to perform filtering please issue the query to the static index.");
        }

        private void ExecuteQueryOperation(QueryMetadata queryMetadata, Func<(QueryRunner Query, CollectionRunner Collection), (QueryOperationOptions Query, CollectionOperationOptions Collection), Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation, DocumentsOperationContext context, IDisposable returnContextToPool, Operations.Operations.OperationType operationType)
        {
            var options = GetQueryOperationOptions();
            var token = CreateTimeLimitedOperationToken();

            var operationId = Database.Operations.GetNextOperationId();

            Task<IOperationResult> task;

            if (queryMetadata.IsDynamic == false)
            {
                var queryRunner = new QueryRunner(Database, context);
                task = Database.Operations.AddOperation(Database, queryMetadata.IndexName, operationType,
                    onProgress => operation((queryRunner, null), (options, null), onProgress, token), operationId, token);
            }
            else
            {
                var collectionRunner = new CollectionRunner(Database, context);

                task = Database.Operations.AddOperation(Database, queryMetadata.CollectionName, operationType,
                    onProgress => operation((null, collectionRunner), (null, new CollectionOperationOptions()
                    {
                        MaxOpsPerSecond = options.MaxOpsPerSecond
                    }), onProgress, token), operationId, token);
            }

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationId(context, operationId);
            }

            task.ContinueWith(_ =>
            {
                using (returnContextToPool)
                    token.Dispose();
            });
        }

        private async Task Debug(DocumentsOperationContext context, string debug, OperationCancelToken token, HttpMethod method)
        {
            if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
            {
                await IndexEntries(context, token, method);
                return;
            }

            if (string.Equals(debug, "explain", StringComparison.OrdinalIgnoreCase))
            {
                await Explain(context, method);
                return;
            }

            throw new NotSupportedException($"Not supported query debug operation: '{debug}'");
        }

        private async Task IndexEntries(DocumentsOperationContext context, OperationCancelToken token, HttpMethod method)
        {
            var indexQuery = await GetIndexQuery(context, method);
            var existingResultEtag = GetLongFromHeaders("If-None-Match");

            var queryRunner = new QueryRunner(Database, context);

            var result = await queryRunner.ExecuteIndexEntriesQuery(indexQuery, existingResultEtag, token);

            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteIndexEntriesQueryResult(context, result);
            }
        }

        private QueryOperationOptions GetQueryOperationOptions()
        {
            return new QueryOperationOptions
            {
                AllowStale = GetBoolValueQueryString("allowStale", required: false) ?? false,
                MaxOpsPerSecond = GetIntValueQueryString("maxOpsPerSec", required: false),
                StaleTimeout = GetTimeSpanQueryString("staleTimeout", required: false),
                RetrieveDetails = GetBoolValueQueryString("details", required: false) ?? false
            };
        }
    }
}
