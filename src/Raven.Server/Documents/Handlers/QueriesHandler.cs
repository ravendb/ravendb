﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Handlers
{
    public class QueriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/queries", "POST", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public Task Post()
        {
            return HandleQuery(HttpMethod.Post);
        }

        [RavenAction("/databases/*/queries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public Task Get()
        {
            return HandleQuery(HttpMethod.Get);
        }

        public async Task HandleQuery(HttpMethod httpMethod)
        {
            using (var tracker = new RequestTimeTracker(HttpContext, Logger, Database, "Query"))
            {
                try
                {
                    using (var token = CreateHttpRequestBoundTimeLimitedOperationTokenForQuery())
                    using (var queryContext = QueryOperationContext.Allocate(Database))
                    {
                        var debug = GetStringQueryString("debug", required: false);
                        if (string.IsNullOrWhiteSpace(debug) == false)
                        {
                            await Debug(queryContext, debug, token, tracker, httpMethod);
                            
                            tracker.Dispose();
                            
                            return;
                        }

                        await Query(queryContext, token, tracker, httpMethod);
                        
                        tracker.Dispose();
                    }
                }
                catch (Exception e)
                {
                    if (tracker.Query == null)
                    {
                        string errorMessage;
                        if (e is EndOfStreamException || e is ArgumentException)
                        {
                            errorMessage = "Failed: " + e.Message;
                        }
                        else
                        {
                            errorMessage = "Failed: " +
                                           HttpContext.Request.Path.Value +
                                           e.ToString();
                        }

                        tracker.Query = errorMessage;
                        if (TrafficWatchManager.HasRegisteredClients)
                            AddStringToHttpContext(errorMessage, TrafficWatchChangeType.Queries);
                    }

                    throw;
                }
            }
        }

        private async Task FacetedQuery(IndexQueryServerSide indexQuery, QueryOperationContext queryContext, OperationCancelToken token)
        {
            var existingResultEtag = GetLongFromHeaders(Constants.Headers.IfNoneMatch);

            var result = await Database.QueryRunner.ExecuteFacetedQuery(indexQuery, existingResultEtag, queryContext, token);

            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

            long numberOfResults;
            await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, ResponseBodyStream()))
            {
                result.Timings = indexQuery.Timings?.ToTimings();
                numberOfResults = await writer.WriteFacetedQueryResultAsync(queryContext.Documents, result, token.Token);
            }

            Database.QueryMetadataCache.MaybeAddToCache(indexQuery.Metadata, result.IndexName);
            
            if (ShouldAddPagingPerformanceHint(numberOfResults))
                AddPagingPerformanceHint(PagingOperationType.Queries, $"{nameof(FacetedQuery)} ({result.IndexName})", $"{indexQuery.Metadata.QueryText}\n{indexQuery.QueryParameters}", numberOfResults, indexQuery.PageSize, result.DurationInMs, -1);

            AddQueryTimingsToTrafficWatch(indexQuery);
        }

        private async Task Query(QueryOperationContext queryContext, OperationCancelToken token, RequestTimeTracker tracker, HttpMethod method)
        {
            var indexQuery = await GetIndexQuery(queryContext.Documents, method, tracker);
            
            indexQuery.Diagnostics = GetBoolValueQueryString("diagnostics", required: false) ?? false ? new List<string>() : null;
            indexQuery.AddTimeSeriesNames = GetBoolValueQueryString("addTimeSeriesNames", false) ?? false;
            indexQuery.DisableAutoIndexCreation = GetBoolValueQueryString("disableAutoIndexCreation", false) ?? false;

            queryContext.WithQuery(indexQuery.Metadata);

            if (TrafficWatchManager.HasRegisteredClients)
                TrafficWatchQuery(indexQuery);

            var existingResultEtag = GetLongFromHeaders(Constants.Headers.IfNoneMatch);

            if (indexQuery.Metadata.HasFacet)
            {
                await FacetedQuery(indexQuery, queryContext, token);
                return;
            }

            if (indexQuery.Metadata.HasSuggest)
            {
                await SuggestQuery(indexQuery, queryContext, token);
                return;
            }

            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;
            var shouldReturnServerSideQuery = GetBoolValueQueryString("includeServerSideQuery", required: false) ?? false;

            DocumentQueryResult result;
            try
            {
                result = await Database.QueryRunner.ExecuteQuery(indexQuery, queryContext, existingResultEtag, token).ConfigureAwait(false);
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

            long numberOfResults;
            long totalDocumentsSizeInBytes;
            await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, ResponseBodyStream()))
            {
                result.Timings = indexQuery.Timings?.ToTimings();
                (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentQueryResultAsync(queryContext.Documents, result, metadataOnly, WriteAdditionalData(indexQuery, shouldReturnServerSideQuery), token.Token);
            }

            Database.QueryMetadataCache.MaybeAddToCache(indexQuery.Metadata, result.IndexName);
            
            if (ShouldAddPagingPerformanceHint(numberOfResults))
                AddPagingPerformanceHint(PagingOperationType.Queries, $"{nameof(Query)} ({result.IndexName})", $"{indexQuery.Metadata.QueryText}\n{indexQuery.QueryParameters}", numberOfResults, indexQuery.PageSize, result.DurationInMs, totalDocumentsSizeInBytes);

            AddQueryTimingsToTrafficWatch(indexQuery);
        }

        private void AddQueryTimingsToTrafficWatch(IndexQueryServerSide indexQuery)
        {
            if (TrafficWatchManager.HasRegisteredClients && indexQuery.Timings != null)
                HttpContext.Items[nameof(QueryTimings)] = indexQuery.Timings.ToTimings();
        }

        private Action<AbstractBlittableJsonTextWriter> WriteAdditionalData(IndexQueryServerSide indexQuery, bool shouldReturnServerSideQuery)
        {
            if (indexQuery.Diagnostics == null && shouldReturnServerSideQuery == false)
                return null;

            return w =>
            {
                if (shouldReturnServerSideQuery)
                {
                    w.WriteComma();
                    w.WritePropertyName(nameof(indexQuery.ServerSideQuery));
                    w.WriteString(indexQuery.ServerSideQuery);
                }

                if (indexQuery.Diagnostics != null)
                {
                    w.WriteComma();
                    w.WriteArray(nameof(indexQuery.Diagnostics), indexQuery.Diagnostics);
                }
            };
        }

        private async Task<IndexQueryServerSide> GetIndexQuery(JsonOperationContext context, HttpMethod method, RequestTimeTracker tracker)
        {
            var addSpatialProperties = GetBoolValueQueryString("addSpatialProperties", required: false) ?? false;
            var clientQueryId = GetStringQueryString("clientQueryId", required: false);

            if (method == HttpMethod.Get)
                return await IndexQueryServerSide.CreateAsync(HttpContext, GetStart(), GetPageSize(), context, tracker, addSpatialProperties, clientQueryId);

            var json = await context.ReadForMemoryAsync(RequestBodyStream(), "index/query");

            return IndexQueryServerSide.Create(HttpContext, json, Database.QueryMetadataCache, tracker, addSpatialProperties, clientQueryId, Database);
        }

        private async Task SuggestQuery(IndexQueryServerSide indexQuery, QueryOperationContext queryContext, OperationCancelToken token)
        {
            var existingResultEtag = GetLongFromHeaders(Constants.Headers.IfNoneMatch);
            var result = await Database.QueryRunner.ExecuteSuggestionQuery(indexQuery, queryContext, existingResultEtag, token);
            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

            long numberOfResults;
            long totalDocumentsSizeInBytes;
            await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, ResponseBodyStream()))
            {
                (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteSuggestionQueryResultAsync(queryContext.Documents, result, token.Token);
            }

            if (ShouldAddPagingPerformanceHint(numberOfResults))
                AddPagingPerformanceHint(PagingOperationType.Queries, $"{nameof(SuggestQuery)} ({result.IndexName})", indexQuery.Query, numberOfResults, indexQuery.PageSize, result.DurationInMs, totalDocumentsSizeInBytes);
        }

        private async Task DetailedGraphResult(QueryOperationContext queryContext, RequestTimeTracker tracker, HttpMethod method)
        {
            var indexQuery = await GetIndexQuery(queryContext.Documents, method, tracker);
            var queryRunner = Database.QueryRunner.GetRunner(indexQuery);
            if (!(queryRunner is GraphQueryRunner gqr))
                throw new InvalidOperationException("The specified query is not a graph query.");
            using (var token = CreateHttpRequestBoundTimeLimitedOperationTokenForQuery())
            await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, ResponseBodyStream()))
            {
                await gqr.WriteDetailedQueryResult(indexQuery, queryContext, writer, token);
            }
        }

        private async Task Graph(QueryOperationContext queryContext, RequestTimeTracker tracker, HttpMethod method)
        {
            var indexQuery = await GetIndexQuery(queryContext.Documents, method, tracker);
            var queryRunner = Database.QueryRunner.GetRunner(indexQuery);
            if (!(queryRunner is GraphQueryRunner gqr))
                throw new InvalidOperationException("The specified query is not a graph query.");

            using (var token = CreateHttpRequestBoundTimeLimitedOperationTokenForQuery())
            {
                var results = await gqr.GetAnalyzedQueryResults(indexQuery, queryContext, null, token);

                var nodes = new DynamicJsonArray();
                var edges = new DynamicJsonArray();
                var output = new DynamicJsonValue
                {
                    ["Nodes"] = nodes,
                    ["Edges"] = edges
                };

                foreach (var item in results.Nodes)
                {
                    var val = item.Value;
                    if (val is Document d)
                    {
                        d.EnsureMetadata();
                        val = d.Data;
                    }
                    nodes.Add(new DynamicJsonValue
                    {
                        ["Id"] = item.Key,
                        ["Value"] = val
                    });
                }
                var added = new HashSet<(string, string, object)>();
                foreach (var edge in results.Edges)
                {
                    added.Clear();
                    var array = new DynamicJsonArray();
                    var djv = new DynamicJsonValue
                    {
                        ["Name"] = edge.Key,
                        ["Results"] = array
                    };
                    foreach (var item in edge.Value)
                    {
                        var edgeVal = item.Edge;
                        if (edgeVal is Document d)
                        {
                            edgeVal = d.Id?.ToString() ?? "anonymous/" + Guid.NewGuid();
                        }
                        if (added.Add((item.Source, item.Destination, edgeVal)) == false)
                            continue;
                        array.Add(new DynamicJsonValue
                        {
                            ["From"] = item.Source,
                            ["To"] = item.Destination,
                            ["Edge"] = edgeVal
                        });
                    }
                    edges.Add(djv);
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, ResponseBodyStream()))
                {
                    queryContext.Documents.Write(writer, output);
                }
            }
        }

        private async Task Explain(QueryOperationContext queryContext, RequestTimeTracker tracker, HttpMethod method)
        {
            var indexQuery = await GetIndexQuery(queryContext.Documents, method, tracker);

            var explanations = Database.QueryRunner.ExplainDynamicIndexSelection(indexQuery, out string indexName);

            await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("IndexName");
                writer.WriteString(indexName);
                writer.WriteComma();
                writer.WriteArray(queryContext.Documents, "Results", explanations, (w, c, explanation) => w.WriteExplanation(queryContext.Documents, explanation));

                writer.WriteEndObject();
            }
        }

        private async Task ServerSideQuery(QueryOperationContext queryContext, RequestTimeTracker tracker, HttpMethod method)
        {
            var indexQuery = await GetIndexQuery(queryContext.Documents, method, tracker);
            await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName(nameof(indexQuery.ServerSideQuery));
                writer.WriteString(indexQuery.ServerSideQuery);

                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/queries", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task Delete()
        {
            var queryContext = QueryOperationContext.Allocate(Database); // we don't dispose this as operation is async

            try
            {
                using (var tracker = new RequestTimeTracker(HttpContext, Logger, Database, "DeleteByQuery"))
                {
                    var reader = await queryContext.Documents.ReadForMemoryAsync(RequestBodyStream(), "queries/delete");
                    var query = IndexQueryServerSide.Create(HttpContext, reader, Database.QueryMetadataCache, tracker);

                    if (TrafficWatchManager.HasRegisteredClients)
                        TrafficWatchQuery(query);
                    
                    if (LoggingSource.AuditLog.IsInfoEnabled)
                        LogAuditFor(Database.Name, "DELETE", $"Documents matching the query: {query}");

                    await ExecuteQueryOperation(query,
                        (runner, options, onProgress, token) => runner.ExecuteDeleteQuery(query, options, queryContext, onProgress, token),
                        queryContext, Operations.Operations.OperationType.DeleteByQuery);
                }
            }
            catch
            {
                queryContext.Dispose();
                throw;
            }
        }

        [RavenAction("/databases/*/queries/test", "PATCH", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task PatchTest()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var reader = await context.ReadForMemoryAsync(RequestBodyStream(), "queries/patch");
                if (reader == null)
                    throw new BadRequestException("Missing JSON content.");
                if (reader.TryGet("Query", out BlittableJsonReaderObject queryJson) == false || queryJson == null)
                    throw new BadRequestException("Missing 'Query' property.");

                var query = IndexQueryServerSide.Create(HttpContext, queryJson, Database.QueryMetadataCache, null, queryType: QueryType.Update);

                if (TrafficWatchManager.HasRegisteredClients)
                    TrafficWatchQuery(query);

                var patch = new PatchRequest(query.Metadata.GetUpdateBody(query.QueryParameters), PatchRequestType.Patch, query.Metadata.DeclaredFunctions);

                var docId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

                var command = new PatchDocumentCommand(context, docId,
                    expectedChangeVector: null,
                    skipPatchIfChangeVectorMismatch: false,
                    patch: (patch, query.QueryParameters),
                    patchIfMissing: (null, null),
                    createIfMissing: null,
                    database: context.DocumentDatabase,
                    debugMode: true,
                    isTest: true,
                    collectResultsNeeded: true,
                    returnDocument: false

                );

                using (context.OpenWriteTransaction())
                {
                    command.Execute(context, null);
                }

                switch (command.PatchResult.Status)
                {
                    case PatchStatus.DocumentDoesNotExist:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        return;


                    case PatchStatus.Created:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                        break;

                    case PatchStatus.Skipped:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                        return;


                    case PatchStatus.Patched:
                    case PatchStatus.NotModified:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }

                await WritePatchResultToResponseAsync(context, command);
            }
        }

        private async ValueTask WritePatchResultToResponseAsync(DocumentsOperationContext context, PatchDocumentCommand command)
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
                    ["Output"] = new DynamicJsonArray(command.DebugOutput),
                    ["Actions"] = command.DebugActions
                });

                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/queries", "PATCH", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Patch()
        {
            var queryContext = QueryOperationContext.Allocate(Database); // we don't dispose this as operation is async
            
            try
            {
                var reader = await queryContext.Documents.ReadForMemoryAsync(RequestBodyStream(), "queries/patch");
                if (reader == null)
                    throw new BadRequestException("Missing JSON content.");
                if (reader.TryGet("Query", out BlittableJsonReaderObject queryJson) == false || queryJson == null)
                    throw new BadRequestException("Missing 'Query' property.");

                var query = IndexQueryServerSide.Create(HttpContext, queryJson, Database.QueryMetadataCache, null, queryType: QueryType.Update);
                
                query.DisableAutoIndexCreation = GetBoolValueQueryString("disableAutoIndexCreation", false) ?? false;

                if (TrafficWatchManager.HasRegisteredClients)
                    TrafficWatchQuery(query);

                var patch = new PatchRequest(query.Metadata.GetUpdateBody(query.QueryParameters), PatchRequestType.Patch, query.Metadata.DeclaredFunctions);

                await ExecuteQueryOperation(query,
                    (runner, options, onProgress, token) => runner.ExecutePatchQuery(
                        query, options, patch, query.QueryParameters, queryContext, onProgress, token),
                    queryContext, Operations.Operations.OperationType.UpdateByQuery);
            }
            catch
            {
                queryContext.Dispose();
                throw;
            }
        }

        /// <summary>
        /// TrafficWatchQuery writes query data to httpContext
        /// </summary>
        /// <param name="indexQuery"></param>
        private void TrafficWatchQuery(IndexQueryServerSide indexQuery)
        {
            var sb = new StringBuilder();
            // append stringBuilder with the query
            sb.Append(indexQuery.Query);
            // if query got parameters append with parameters
            if (indexQuery.QueryParameters != null && indexQuery.QueryParameters.Count > 0)
                sb.AppendLine().Append(indexQuery.QueryParameters);
            AddStringToHttpContext(sb.ToString(), TrafficWatchChangeType.Queries);
        }

        private async Task ExecuteQueryOperation(IndexQueryServerSide query,
                Func<QueryRunner,
                QueryOperationOptions,
                Action<IOperationProgress>, OperationCancelToken,
                Task<IOperationResult>> operation,
                IDisposable returnContextToPool,
                Operations.Operations.OperationType operationType)
        {
            var options = GetQueryOperationOptions();
            var token = CreateTimeLimitedBackgroundOperationTokenForQueryOperation();

            var operationId = Database.Operations.GetNextOperationId();

            var indexName = query.Metadata.IsDynamic
                ? (query.Metadata.IsCollectionQuery ? "collection/" : "dynamic/") + query.Metadata.CollectionName
                : query.Metadata.IndexName;

            var details = new BulkOperationResult.OperationDetails
            {
                Query = query.QueryParameters?.Count > 0 ? $"{query.Query}{Environment.NewLine}{query.QueryParameters}" : query.Query
            };

            var task = Database.Operations.AddOperation(
                Database,
                indexName,
                operationType,
                onProgress => operation(Database.QueryRunner, options, onProgress, token), operationId, details, token: token);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
            }

            _ = task.ContinueWith(_ =>
            {
                using (returnContextToPool)
                    token.Dispose();
            });
        }

        private async Task Debug(QueryOperationContext queryContext, string debug, OperationCancelToken token, RequestTimeTracker tracker, HttpMethod method)
        {
            if (string.Equals(debug, "entries", StringComparison.OrdinalIgnoreCase))
            {
                var ignoreLimit = GetBoolValueQueryString("ignoreLimit", required: false) ?? false;
                await IndexEntries(queryContext, token, tracker, method, ignoreLimit);
                return;
            }

            if (string.Equals(debug, "explain", StringComparison.OrdinalIgnoreCase))
            {
                await Explain(queryContext, tracker, method);
                return;
            }

            if (string.Equals(debug, "serverSideQuery", StringComparison.OrdinalIgnoreCase))
            {
                await ServerSideQuery(queryContext, tracker, method);
                return;
            }

            if (string.Equals(debug, "graph", StringComparison.OrdinalIgnoreCase))
            {
                await Graph(queryContext, tracker, method);
                return;
            }

            if (string.Equals(debug, "detailedGraphResult", StringComparison.OrdinalIgnoreCase))
            {
                await DetailedGraphResult(queryContext, tracker, method);
                return;
            }
            throw new NotSupportedException($"Not supported query debug operation: '{debug}'");
        }

        private async Task IndexEntries(QueryOperationContext queryContext, OperationCancelToken token, RequestTimeTracker tracker, HttpMethod method, bool ignoreLimit)
        {
            var indexQuery = await GetIndexQuery(queryContext.Documents, method, tracker);
            var existingResultEtag = GetLongFromHeaders(Constants.Headers.IfNoneMatch);

            var result = await Database.QueryRunner.ExecuteIndexEntriesQuery(indexQuery, queryContext, ignoreLimit, existingResultEtag, token);

            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;
            }

            HttpContext.Response.Headers[Constants.Headers.Etag] = CharExtensions.ToInvariantString(result.ResultEtag);

            await using (var writer = new AsyncBlittableJsonTextWriter(queryContext.Documents, ResponseBodyStream()))
            {
                await writer.WriteIndexEntriesQueryResultAsync(queryContext.Documents, result, token.Token);
            }
        }

        private QueryOperationOptions GetQueryOperationOptions()
        {
            return new QueryOperationOptions
            {
                AllowStale = GetBoolValueQueryString("allowStale", required: false) ?? false,
                MaxOpsPerSecond = GetIntValueQueryString("maxOpsPerSec", required: false),
                StaleTimeout = GetTimeSpanQueryString("staleTimeout", required: false),
                RetrieveDetails = GetBoolValueQueryString("details", required: false) ?? false,
                IgnoreMaxStepsForScript = GetBoolValueQueryString("ignoreMaxStepsForScript", required: false) ?? false
            };
        }
    }
}
