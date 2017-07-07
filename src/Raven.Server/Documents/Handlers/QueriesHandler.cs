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
using Raven.Server.Documents.Indexes.IndexMerging;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Faceted;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Suggestions;
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
        [RavenAction("/databases/*/queries/$", "POST", AuthorizationStatus.ValidUser)]
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

                if (string.Equals(operation, "facets", StringComparison.OrdinalIgnoreCase))
                {
                    await FacetedQuery(context, token).ConfigureAwait(false);
                    return;
                }

                await Query(context, token, HttpMethod.Post).ConfigureAwait(false);
            }
        }

        [RavenAction("/databases/*/queries/$", "GET", AuthorizationStatus.ValidUser)]
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
                    MoreLikeThis(context, token);
                    return;
                }

                if (string.Equals(operation, "facets", StringComparison.OrdinalIgnoreCase))
                {
                    await FacetedQuery(context, token).ConfigureAwait(false);
                    return;
                }

                if (string.Equals(operation, "suggest", StringComparison.OrdinalIgnoreCase))
                {
                    Suggest(context, indexName, token);
                    return;

                }

                await Query(context, indexName, token, HttpMethod.Get).ConfigureAwait(false);
            }
        }

        private async Task FacetedQuery(DocumentsOperationContext context, OperationCancelToken token)
        {
            var query = FacetQuery.Parse(HttpContext.Request.Query, GetStart(), GetPageSize(), DocumentConventions.Default);

            var existingResultEtag = GetLongFromHeaders("If-None-Match");
            long? facetsEtag = null;
            if (query.FacetSetupDoc == null)
            {
                KeyValuePair<List<Facet>, long> facets;
                if (HttpContext.Request.Method == HttpMethod.Post.Method)
                {
                    var input = await context.ReadForMemoryAsync(RequestBodyStream(), "facets");
                    if (input.TryGet("Facets", out BlittableJsonReaderArray array) == false)
                        ThrowRequiredPropertyNameInRequest("Facets");
                    facets = FacetedQueryParser.ParseFromJson(array);
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

            var result = await runner.ExecuteFacetedQuery(query, facetsEtag, existingResultEtag, token);

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

        private async Task Query(DocumentsOperationContext context, OperationCancelToken token, HttpMethod method)
        {
            var indexQuery = GetIndexQuery(context, method);

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

            AddPagingPerformanceHint(PagingOperationType.Queries, $"{nameof(Query)} ({indexQuery.GetIndex()})", HttpContext, numberOfResults, indexQuery.PageSize, TimeSpan.FromMilliseconds(result.DurationInMs));
        }

        private IndexQueryServerSide GetIndexQuery(DocumentsOperationContext context, HttpMethod method)
        {
            var indexQuery = IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(), context);

            if (method == HttpMethod.Post && string.IsNullOrWhiteSpace(indexQuery.Query))
            {
                var request = context.Read(RequestBodyStream(), "QueryInPostBody");
                if (request.TryGet("Query", out string queryString) == false)
                    throw new InvalidDataException("Missing 'Query' property in the POST request body");
                indexQuery.Query = queryString;
            }
            return indexQuery;
        }

        private void Suggest(DocumentsOperationContext context, string indexName, OperationCancelToken token)
        {
            var existingResultEtag = GetLongFromHeaders("If-None-Match");

            var query = SuggestionQueryServerSide.Create(HttpContext, GetPageSize(), context);
            var runner = new QueryRunner(Database, context);

            var result = runner.ExecuteSuggestionQuery(indexName, query, context, existingResultEtag, token);
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

            AddPagingPerformanceHint(PagingOperationType.Queries, $"{nameof(Suggest)} ({indexName})", HttpContext, result.Suggestions.Length, query.PageSize, TimeSpan.FromMilliseconds(result.DurationInMs));
        }

        private void MoreLikeThis(DocumentsOperationContext context, string indexName, OperationCancelToken token)
        {
            var existingResultEtag = GetLongFromHeaders("If-None-Match");

            var query = MoreLikeThisQueryServerSide.Create(HttpContext, GetPageSize(), context);
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

            AddPagingPerformanceHint(PagingOperationType.Queries, $"{nameof(MoreLikeThis)} ({query.GetIndex()})", HttpContext, numberOfResults, query.PageSize, TimeSpan.FromMilliseconds(result.DurationInMs));
        }

        private void Explain(DocumentsOperationContext context)
        {
            var indexQuery = IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(), context);
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

        [RavenAction("/databases/*/queries/$", "DELETE", AuthorizationStatus.ValidUser)]
        public Task Delete()
        {
            var returnContextToPool = ContextPool.AllocateOperationContext(out DocumentsOperationContext context); // we don't dispose this as operation is async

            ExecuteQueryOperation((runner, indexName, query, options, onProgress, token) => runner.ExecuteDeleteQuery(indexName, query, options, context, onProgress, token),
                context, returnContextToPool, Operations.Operations.OperationType.DeleteByIndex);
            return Task.CompletedTask;

        }

        [RavenAction("/databases/*/queries/$", "PATCH", AuthorizationStatus.ValidUser)]
        public Task Patch()
        {
            var returnContextToPool = ContextPool.AllocateOperationContext(out DocumentsOperationContext context); // we don't dispose this as operation is async

            var reader = context.Read(RequestBodyStream(), "ScriptedPatchRequest");
            var patch = PatchRequest.Parse(reader);

            ExecuteQueryOperation((runner, indexName, query, options, onProgress, token) => runner.ExecutePatchQuery(indexName, query, options, patch, context, onProgress, token),
                context, returnContextToPool, Operations.Operations.OperationType.UpdateByIndex);
            return Task.CompletedTask;
        }

        private void ExecuteQueryOperation(Func<QueryRunner, string, IndexQueryServerSide, QueryOperationOptions, Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation, DocumentsOperationContext context, IDisposable returnContextToPool, Operations.Operations.OperationType operationType)
        {
            var indexName = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            var query = IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(), context);
            var options = GetQueryOperationOptions();
            var token = CreateTimeLimitedOperationToken();

            var queryRunner = new QueryRunner(Database, context);

            var operationId = Database.Operations.GetNextOperationId();

            var task = Database.Operations.AddOperation(Database,indexName, operationType, onProgress =>
                    operation(queryRunner, indexName, query, options, onProgress, token), operationId, token);

            task.ContinueWith(_ =>
            {
                using (returnContextToPool)
                    token.Dispose();
            });

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationId(context, operationId);
            }
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
                Explain(context);
                return;
            }


            throw new NotSupportedException($"Not supported query debug operation: '{debug}'");
        }

        private async Task IndexEntries(DocumentsOperationContext context, OperationCancelToken token, HttpMethod method)
        {
            var indexQuery = GetIndexQuery(context, method);
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