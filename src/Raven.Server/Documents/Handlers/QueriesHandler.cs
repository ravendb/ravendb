using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Json;
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
                var operation = GetStringQueryString("op", required: false);

                if (string.Equals(operation, "facets", StringComparison.OrdinalIgnoreCase))
                {
                    await FacetedQuery(context, indexName, token).ConfigureAwait(false);
                    return;
                }

                throw new NotSupportedException($"Operation '{operation}' is not supported.");
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
                var operation = GetStringQueryString("op", required: false);
                if (string.Equals(operation, "morelikethis", StringComparison.OrdinalIgnoreCase))
                {
                    MoreLikeThis(context, indexName, token);
                    return;
                }

                if (string.Equals(operation, "explain", StringComparison.OrdinalIgnoreCase))
                {
                    Explain(context, indexName);
                    return;
                }

                if (string.Equals(operation, "facets", StringComparison.OrdinalIgnoreCase))
                {
                    await FacetedQuery(context, indexName, token).ConfigureAwait(false);
                    return;
                }

                await Query(context, indexName, token).ConfigureAwait(false);
            }
        }

        private async Task FacetedQuery(DocumentsOperationContext context, string indexName, OperationCancelToken token)
        {
            var query = FacetQuery.Parse(HttpContext.Request.Query, GetStart(), GetPageSize(Database.Configuration.Core.MaxPageSize));

            var existingResultEtag = GetLongFromHeaders("If-None-Match");
            long? facetsEtag = null;
            if (query.FacetSetupDoc == null)
            {
                string f;
                if (HttpContext.Request.Method == HttpMethod.Post.Method)
                {
                    // TODO [ppekrol] fix me: how to calculate XXHash from blittable array?
                    using (var reader = new StreamReader(RequestBodyStream()))
                        f = reader.ReadToEnd();
                }
                else if (HttpContext.Request.Method == HttpMethod.Get.Method)
                {
                    f = GetStringQueryString("facets");
                }
                else
                    throw new NotSupportedException($"Unsupported HTTP method '{HttpContext.Request.Method}' for Faceted Query.");

                if (string.IsNullOrWhiteSpace(f))
                    throw new InvalidOperationException($"One of the required parameters (facetDoc or facets) was not specified.");

                var facets = await FacetedQueryParser.ParseFromStringAsync(f, context);
                facetsEtag = facets.Value;
                query.Facets = facets.Key;
            }

            var runner = new QueryRunner(Database, context);

            var result = await runner.ExecuteFacetedQuery(indexName, query, facetsEtag, existingResultEtag, token);

            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = 304;
                return;
            }

            HttpContext.Response.Headers[Constants.MetadataEtagField] = result.ResultEtag.ToInvariantString();

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteFacetedQueryResult(context, result);
            }
        }

        private async Task Query(DocumentsOperationContext context, string indexName, OperationCancelToken token)
        {
            var query = IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(Database.Configuration.Core.MaxPageSize), context);

            var existingResultEtag = GetLongFromHeaders("If-None-Match");
            var includes = GetStringValuesQueryString("include", required: false);
            var metadataOnly = GetBoolValueQueryString("metadata-only", required: false) ?? false;

            var runner = new QueryRunner(Database, context);

            var result = await runner.ExecuteQuery(indexName, query, includes, existingResultEtag, token).ConfigureAwait(false);

            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = 304;
                return;
            }

            HttpContext.Response.Headers[Constants.MetadataEtagField] = result.ResultEtag.ToInvariantString();

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteDocumentQueryResult(context, result, metadataOnly);
            }
        }

        private void MoreLikeThis(DocumentsOperationContext context, string indexName, OperationCancelToken token)
        {
            var existingResultEtag = GetLongFromHeaders("If-None-Match");

            var query = GetMoreLikeThisQuery(context);
            var runner = new QueryRunner(Database, context);

            var result = runner.ExecuteMoreLikeThisQuery(indexName, query, context, existingResultEtag, token);

            if (result.NotModified)
            {
                HttpContext.Response.StatusCode = 304;
                return;
            }

            HttpContext.Response.Headers[Constants.MetadataEtagField] = result.ResultEtag.ToInvariantString();

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteQueryResult(context, result, metadataOnly: false);
            }
        }

        private void Explain(DocumentsOperationContext context, string indexName)
        {
            var indexQuery = IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(Database.Configuration.Core.MaxPageSize), context);
            var runner = new QueryRunner(Database, context);

            var explanations = runner.ExplainDynamicIndexSelection(indexName, indexQuery);

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var isFirst = true;
                writer.WriteStartArray();
                foreach (var explanation in explanations)
                {
                    if (isFirst == false)
                        writer.WriteComma();

                    isFirst = false;
                    writer.WriteExplanation(context, explanation);
                }
                writer.WriteEndArray();
            }
        }

        [RavenAction("/databases/*/queries-delete-by-index/$", "GET")]
        public async Task Delete()
        {
            using (var socket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                DocumentsOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    await ExecuteQueryOperation((runner, indexName, query, options, onProgress, token) => runner.ExecuteDeleteQuery(indexName, query, options, context, onProgress, token),
                    context, DatabaseOperations.PendingOperationType.DeleteByIndex, socket);
                }
            }
        }

        [RavenAction("/databases/*/queries-update-by-index/$", "GET")]
        public async Task Patch()
        {
            using (var socket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                DocumentsOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    var reader = await context.ReadFromWebSocket(socket, "ScriptedPatchRequest", CancellationToken.None);
                    var patch = PatchRequest.Parse(reader);

                    await ExecuteQueryOperation((runner, indexName, query, options, onProgress, token) => runner.ExecutePatchQuery(indexName, query, options, patch, context, onProgress, token),
                        context, DatabaseOperations.PendingOperationType.UpdateByIndex, socket);
                }
            }
        }

        private async Task ExecuteQueryOperation(Func<QueryRunner, string, IndexQueryServerSide, QueryOperationOptions, Action<IOperationProgress>, OperationCancelToken, Task<IOperationResult>> operation, 
            DocumentsOperationContext context, DatabaseOperations.PendingOperationType operationType, WebSocket socket)
        {
            var indexName = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            var query = IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(int.MaxValue), context);
            var options = GetQueryOperationOptions();
            var token = CreateTimeLimitedOperationToken();

            var queryRunner = new QueryRunner(Database, context);

            await Database.DatabaseOperations.ExecuteOperation(indexName, operationType, context, onProgress => operation(queryRunner, indexName, query, options, onProgress, token), socket, token);
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

        private MoreLikeThisQueryServerSide GetMoreLikeThisQuery(JsonOperationContext context)
        {
            var result = new MoreLikeThisQueryServerSide
            {
                Fields = GetStringValuesQueryString("fields", required: false),
                Boost = GetBoolValueQueryString("boost", required: false),
                BoostFactor = GetFloatValueQueryString("boostFactor", required: false),
                MaximumNumberOfTokensParsed = GetIntValueQueryString("maxNumTokens", required: false),
                MaximumQueryTerms = GetIntValueQueryString("maxQueryTerms", required: false),
                MaximumWordLength = GetIntValueQueryString("maxWordLen", required: false),
                MinimumDocumentFrequency = GetIntValueQueryString("minDocFreq", required: false),
                MaximumDocumentFrequency = GetIntValueQueryString("maxDocFreq", required: false),
                MaximumDocumentFrequencyPercentage = GetIntValueQueryString("maxDocFreqPct", required: false),
                MinimumTermFrequency = GetIntValueQueryString("minTermFreq", required: false),
                MinimumWordLength = GetIntValueQueryString("minWordLen", required: false),
                StopWordsDocumentId = GetStringQueryString("stopWords", required: false),
                AdditionalQuery = GetStringQueryString("query", required: false),
                Includes = GetStringValuesQueryString("include", required: false),
                DocumentId = GetStringQueryString("docId", required: false),
                Transformer = GetStringValuesQueryString("transformer", required: false),
                PageSize = GetPageSize(Database.Configuration.Core.MaxPageSize)
            };

            result.TransformerParameters = new Dictionary<string, object>();
            foreach (var tp in HttpContext.Request.Query.Where(x => x.Key.StartsWith("tp-", StringComparison.OrdinalIgnoreCase)))
            {
                throw new NotImplementedException();
            }

            result.MapGroupFields = new Dictionary<string, string>();
            foreach (var mgf in HttpContext.Request.Query.Where(x => x.Key.StartsWith("mgf-", StringComparison.OrdinalIgnoreCase)))
                result.MapGroupFields[mgf.Key.Substring(4)] = mgf.Value[0];

            return result;
        }
    }
}