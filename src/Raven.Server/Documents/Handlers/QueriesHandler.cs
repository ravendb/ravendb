using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Extensions;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Faceted;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
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
        [RavenAction("/databases/*/queries/$", "POST")]
        public async Task Post()
        {
            var indexName = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            DocumentsOperationContext context;

            using (TrackRequestTime())
            using (var token = CreateTimeLimitedOperationToken())
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                var debug = GetStringQueryString("debug", required: false);
                if (string.IsNullOrWhiteSpace(debug) == false)
                {
                    await Debug(context, indexName, debug, token, HttpMethod.Post);
                    return;
                }

                var operation = GetStringQueryString("op", required: false);

                if (string.Equals(operation, "facets", StringComparison.OrdinalIgnoreCase))
                {
                    await FacetedQuery(context, indexName, token).ConfigureAwait(false);
                    return;
                }

                await Query(context, indexName, token, HttpMethod.Post).ConfigureAwait(false);
            }
        }

        [RavenAction("/databases/*/queries/$", "GET")]
        public async Task Get()
        {
            var indexName = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            DocumentsOperationContext context;
            using (TrackRequestTime())
            using (var token = CreateTimeLimitedOperationToken())
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                var debug = GetStringQueryString("debug", required: false);
                if (string.IsNullOrWhiteSpace(debug) == false)
                {
                    await Debug(context, indexName, debug, token, HttpMethod.Get);
                    return;
                }

                var operation = GetStringQueryString("op", required: false);
                if (string.Equals(operation, "morelikethis", StringComparison.OrdinalIgnoreCase))
                {
                    MoreLikeThis(context, indexName, token);
                    return;
                }

                if (string.Equals(operation, "facets", StringComparison.OrdinalIgnoreCase))
                {
                    await FacetedQuery(context, indexName, token).ConfigureAwait(false);
                    return;
                }

                await Query(context, indexName, token, HttpMethod.Get).ConfigureAwait(false);
            }
        }

        private async Task FacetedQuery(DocumentsOperationContext context, string indexName, OperationCancelToken token)
        {
            var query = FacetQuery.Parse(HttpContext.Request.Query, GetStart(), GetPageSize(), DocumentConventions.Default);

            var existingResultEtag = GetLongFromHeaders("If-None-Match");
            long? facetsEtag = null;
            if (query.FacetSetupDoc == null)
            {
                KeyValuePair<List<Facet>, long> facets;
                if (HttpContext.Request.Method == HttpMethod.Post.Method)
                {
                    var jsonParseResult = await context.ParseArrayToMemoryAsync(RequestBodyStream(), "facets", BlittableJsonDocumentBuilder.UsageMode.None);
                    using (jsonParseResult.Item2)
                    {
                        facets = FacetedQueryParser.ParseFromJson(jsonParseResult.Item1);
                    }

                }
                else if (HttpContext.Request.Method == HttpMethod.Get.Method)
                {
                    var f = GetStringQueryString("facets");
                    if (string.IsNullOrWhiteSpace(f))
                        throw new InvalidOperationException("One of the required parameters (facetDoc or facets) was not specified.");

                    facets = await FacetedQueryParser.ParseFromStringAsync(f, context);
                }
                else
                    throw new NotSupportedException($"Unsupported HTTP method '{HttpContext.Request.Method}' for Faceted Query.");

                facetsEtag = facets.Value;
                query.Facets = facets.Key;
            }

            var runner = new QueryRunner(Database, context);

            var result = await runner.ExecuteFacetedQuery(indexName, query, facetsEtag, existingResultEtag, token);

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
        }

        private async Task Query(DocumentsOperationContext context, string indexName, OperationCancelToken token, HttpMethod method)
        {
            var indexQuery = GetIndexQuery(context, method);

            var existingResultEtag = GetLongFromHeaders("If-None-Match");
            var includes = GetStringValuesQueryString("include", required: false);
            var metadataOnly = GetBoolValueQueryString("metadata-only", required: false) ?? false;

            var runner = new QueryRunner(Database, context);

            DocumentQueryResult result;
            try
            {
                result = await runner.ExecuteQuery(indexName, indexQuery, includes, existingResultEtag, token).ConfigureAwait(false);
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

            AddPagingPerformanceHint(PagingOperationType.Queries, nameof(Query), numberOfResults, indexQuery.PageSize);
        }

        private IndexQueryServerSide GetIndexQuery(DocumentsOperationContext context, HttpMethod method)
        {
            var indexQuery = IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(), context);

            if (method == HttpMethod.Post && string.IsNullOrWhiteSpace(indexQuery.Query))
            {
                string queryString;
                var request = context.Read(RequestBodyStream(), "QueryInPostBody");
                if (request.TryGet("Query", out queryString) == false)
                    throw new InvalidDataException("Missing 'Query' property in the POST request body");
                indexQuery.Query = queryString;
            }
            return indexQuery;
        }

        private void MoreLikeThis(DocumentsOperationContext context, string indexName, OperationCancelToken token)
        {
            var existingResultEtag = GetLongFromHeaders("If-None-Match");

            var query = MoreLikeThisQueryServerSide.Create(HttpContext, GetPageSize(), context);
            var runner = new QueryRunner(Database, context);

            var result = runner.ExecuteMoreLikeThisQuery(indexName, query, context, existingResultEtag, token);

            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

            int numberOfResults;
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteQueryResult(context, result, metadataOnly: false, numberOfResults: out numberOfResults);
            }

            AddPagingPerformanceHint(PagingOperationType.Queries, nameof(MoreLikeThis), numberOfResults, query.PageSize);
        }

        private void Explain(DocumentsOperationContext context, string indexName)
        {
            var indexQuery = IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(), context);
            var runner = new QueryRunner(Database, context);

            var explanations = runner.ExplainDynamicIndexSelection(indexName, indexQuery);

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteArray(context, explanations, (w, c, explanation) =>
                {
                    w.WriteExplanation(context, explanation);
                });
            }
        }

        [RavenAction("/databases/*/queries/$", "DELETE")]
        public Task Delete()
        {
            DocumentsOperationContext context;
            var returnContextToPool = ContextPool.AllocateOperationContext(out context); // we don't dispose this as operation is async

            ExecuteQueryOperation((runner, indexName, query, options, onProgress, token) => runner.ExecuteDeleteQuery(indexName, query, options, context, onProgress, token),
                context, returnContextToPool, DatabaseOperations.OperationType.DeleteByIndex);
            return Task.CompletedTask;

        }

        [RavenAction("/databases/*/queries/$", "PATCH")]
        public Task Patch()
        {
            DocumentsOperationContext context;
            var returnContextToPool = ContextPool.AllocateOperationContext(out context); // we don't dispose this as operation is async

            var reader = context.Read(RequestBodyStream(), "ScriptedPatchRequest");
            var patch = PatchRequest.Parse(reader);

            ExecuteQueryOperation((runner, indexName, query, options, onProgress, token) => runner.ExecutePatchQuery(indexName, query, options, patch, context, onProgress, token),
                context, returnContextToPool, DatabaseOperations.OperationType.UpdateByIndex);
            return Task.CompletedTask;
        }

        private void ExecuteQueryOperation(Func<QueryRunner, string, IndexQueryServerSide, QueryOperationOptions, Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation, DocumentsOperationContext context, IDisposable returnContextToPool, DatabaseOperations.OperationType operationType)
        {
            var indexName = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            var query = IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(), context);
            var options = GetQueryOperationOptions();
            var token = CreateTimeLimitedOperationToken();

            var queryRunner = new QueryRunner(Database, context);

            var operationId = Database.Operations.GetNextOperationId();

            var task = Database.Operations.AddOperation(indexName, operationType, onProgress =>
                    operation(queryRunner, indexName, query, options, onProgress, token), operationId, token);

            task.ContinueWith(_ => returnContextToPool.Dispose());

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationId(context, operationId);
            }
        }


        private async Task Debug(DocumentsOperationContext context, string indexName, string debug, OperationCancelToken token, HttpMethod method)
        {
            if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
            {
                await IndexEntries(context, indexName, token, method);
                return;
            }

            if (string.Equals(debug, "explain", StringComparison.OrdinalIgnoreCase))
            {
                Explain(context, indexName);
                return;
            }


            throw new NotSupportedException($"Not supported query debug operation: '{debug}'");
        }

        private async Task IndexEntries(DocumentsOperationContext context, string indexName, OperationCancelToken token, HttpMethod method)
        {
            var indexQuery = GetIndexQuery(context, method);
            var existingResultEtag = GetLongFromHeaders("If-None-Match");

            var queryRunner = new QueryRunner(Database, context);

            var result = await queryRunner.ExecuteIndexEntriesQuery(indexName, indexQuery, existingResultEtag, token);

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