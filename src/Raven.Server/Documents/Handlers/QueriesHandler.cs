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
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Faceted;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Documents.Queries.Suggestion;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
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
                    MoreLikeThis(context, token);
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
                    throw new NotSupportedException("GET requests for MoreLikeThis queries are not supported");
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

            var result = await Database.QueryRunner.ExecuteFacetedQuery(query.FacetQuery, query.FacetsEtag, existingResultEtag, context, token);

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
            var metadataOnly = GetBoolValueQueryString("metadata-only", required: false) ?? false;

            DocumentQueryResult result;
            try
            {
                result = await Database.QueryRunner.ExecuteQuery(indexQuery, context, existingResultEtag, token).ConfigureAwait(false);
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

        private MoreLikeThisQueryServerSide GetMoreLikeThisQuery(JsonOperationContext context)
        {
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
                throw new NotImplementedException("RavenDB-8882");
            }

            var indexQueryJson = context.ReadForMemory(RequestBodyStream(), "suggestion/query");

            // read from cache here

            return SuggestionQueryServerSide.Create(indexQueryJson);
        }

        private void Suggest(DocumentsOperationContext context, OperationCancelToken token, HttpMethod method)
        {
            var existingResultEtag = GetLongFromHeaders("If-None-Match");

            var query = GetSuggestionQuery(context, method);

            var result = Database.QueryRunner.ExecuteSuggestionQuery(query, context, existingResultEtag, token);
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

        private void MoreLikeThis(DocumentsOperationContext context, OperationCancelToken token)
        {
            var existingResultEtag = GetLongFromHeaders("If-None-Match");

            var query = GetMoreLikeThisQuery(context);
            
            var result = Database.QueryRunner.ExecuteMoreLikeThisQuery(query, context, existingResultEtag, token);

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

            var explanations = Database.QueryRunner.ExplainDynamicIndexSelection(indexQuery);

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

            ExecuteQueryOperation(query,
                (runner, options, onProgress, token) => runner.ExecuteDeleteQuery(query, options, context, onProgress, token),
                context, returnContextToPool, Operations.Operations.OperationType.DeleteByIndex);

            return Task.CompletedTask;

        }

        [RavenAction("/databases/*/queries/test", "PATCH", AuthorizationStatus.ValidUser)]
        public Task PatchTest()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var reader = context.Read(RequestBodyStream(), "queries/patch");
                if (reader == null)
                    throw new BadRequestException("Missing JSON content.");
                if (reader.TryGet("Query", out BlittableJsonReaderObject queryJson) == false || queryJson == null)
                    throw new BadRequestException("Missing 'Query' property.");

                var query = IndexQueryServerSide.Create(queryJson, context, Database.QueryMetadataCache, QueryType.Update);
                
                var patch = new PatchRequest(query.Metadata.GetUpdateBody(query.QueryParameters), PatchRequestType.Patch);

                var docId = GetDocumentId(query);

                PatchDocumentCommand command;
                if (query.Metadata.IsDynamic == false)
                {
                    command = new PatchDocumentCommand(context, docId,
                        expectedChangeVector: null,
                        skipPatchIfChangeVectorMismatch: false,
                        patch: (patch, query.QueryParameters),
                        patchIfMissing: (null, null),
                        database: context.DocumentDatabase,
                        debugMode: true,
                        isTest: true);
                }
                else
                {
                    command = new PatchDocumentCommand(context, docId, null, false, (patch, query.QueryParameters), (null, null),
                        Database, true, true);
                }

                using (context.OpenWriteTransaction())
                {
                    command.Execute(context);
                }

                switch (command.PatchResult.Status)
                {
                    case PatchStatus.DocumentDoesNotExist:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return Task.CompletedTask;
                    case PatchStatus.Created:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                        break;
                    case PatchStatus.Skipped:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                        return Task.CompletedTask;
                    case PatchStatus.Patched:
                    case PatchStatus.NotModified:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                WritePatchResultToResponse(context, command);

                return Task.CompletedTask;
            }
        }

        private StringSegment  GetDocumentId(IndexQueryServerSide query)
        {
            if (!(query.Metadata?.Query?.Where is BinaryExpression binaryExpression))
            {
                throw new InvalidOperationException("Patch test query must contain have a where clause with a simple id() equality test, but was " + query.Query);
            }
            if (!(binaryExpression.Left is MethodExpression me) || "id".Equals(me.Name, StringComparison.OrdinalIgnoreCase) == false)
            {
                throw new InvalidOperationException("Patch test query must contain have a where clause with a simple id() equality test, but was " + query.Query);
            }
            if (!(binaryExpression.Right is ValueExpression ve))
            {
                throw new InvalidOperationException("Patch test query must contain have a where clause with an id() equality test to a constant value, but was " + query.Query);
            }

            return ve.Token;
        }

        private void WritePatchResultToResponse(DocumentsOperationContext context, PatchDocumentCommand command)
        {
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(command.PatchResult.Status));
                writer.WriteString(command.PatchResult.Status.ToString());
                writer.WriteComma();

                writer.WritePropertyName(nameof(command.PatchResult.ModifiedDocument));
                writer.WriteObject(command.PatchResult.ModifiedDocument);

                writer.WriteComma();
                writer.WritePropertyName(nameof(command.PatchResult.OriginalDocument));
                writer.WriteObject(command.PatchResult.OriginalDocument);

                writer.WriteComma();

                writer.WritePropertyName(nameof(command.PatchResult.Debug));

                context.Write(writer, new DynamicJsonValue
                {
                    ["Info"] = new DynamicJsonArray(command.DebugOutput),
                    ["Actions"] = command.DebugActions?.GetDebugActions()
                });

                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/queries", "PATCH", AuthorizationStatus.ValidUser)]
        public Task Patch()
        {
            var returnContextToPool = ContextPool.AllocateOperationContext(out DocumentsOperationContext context); // we don't dispose this as operation is async

            var reader = context.Read(RequestBodyStream(), "queries/patch");
            if (reader == null)
                throw new BadRequestException("Missing JSON content.");
            if (reader.TryGet("Query", out BlittableJsonReaderObject queryJson) == false || queryJson == null)
                throw new BadRequestException("Missing 'Query' property.");

            var query = IndexQueryServerSide.Create(queryJson, context, Database.QueryMetadataCache, QueryType.Update);

            var patch = new PatchRequest(query.Metadata.GetUpdateBody(query.QueryParameters), PatchRequestType.Patch);

            ExecuteQueryOperation(query,
                (runner, options, onProgress, token) => runner.ExecutePatchQuery(
                    query, options, patch, query.QueryParameters, context, onProgress, token),
                context, returnContextToPool, Operations.Operations.OperationType.UpdateByIndex);

            return Task.CompletedTask;
        }

        private void ExecuteQueryOperation(IndexQueryServerSide query,
                Func<QueryRunner,
                QueryOperationOptions,
                Action<IOperationProgress>, OperationCancelToken,
                Task<IOperationResult>> operation,
                DocumentsOperationContext context,
                IDisposable returnContextToPool,
                Operations.Operations.OperationType operationType)
        {
            var options = GetQueryOperationOptions();
            var token = CreateTimeLimitedOperationToken();

            var operationId = Database.Operations.GetNextOperationId();

            var task = Database.Operations.AddOperation(Database, query.Metadata.IndexName, operationType,
                onProgress => operation(Database.QueryRunner, options, onProgress, token), operationId, token);

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

            var result = await Database.QueryRunner.ExecuteIndexEntriesQuery(indexQuery, context, existingResultEtag, token);

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
