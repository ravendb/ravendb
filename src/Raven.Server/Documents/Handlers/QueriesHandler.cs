using System;
using System.Collections.Generic;
using System.Linq;
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

                await Query(context, indexName, token).ConfigureAwait(false);
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

        [RavenAction("/databases/*/queries/$", "DELETE")]
        public Task Delete()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                return ExecuteQueryOperation((runner, indexName, query, options, token) => runner.ExecuteDeleteQuery(indexName, query, options, context, token), context);
            }
        }

        [RavenAction("/databases/*/queries/$", "PATCH")]
        public Task Patch()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var reader = context.Read(RequestBodyStream(), "ScriptedPatchRequest");
                var patch = PatchRequest.Parse(reader);

                return ExecuteQueryOperation((runner, indexName, query, options, token) => runner.ExecutePatchQuery(indexName, query, options, patch, context, token), context);
            }
        }

        private async Task ExecuteQueryOperation(Func<QueryRunner, string, IndexQueryServerSide, QueryOperationOptions, OperationCancelToken, Task> operation, DocumentsOperationContext context)
        {
            var indexName = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            var query = IndexQueryServerSide.Create(HttpContext, GetStart(), GetPageSize(int.MaxValue), context);
            var options = GetQueryOperationOptions();
            var token = CreateTimeLimitedOperationToken();

            // TODO [ppekrol] 
            // implement Tasks
            // support RetrieveDetails

            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var queryRunner = new QueryRunner(Database, context);
                await operation(queryRunner, indexName, query, options, token).ConfigureAwait(false);

                writer.WriteStartObject();

                writer.WritePropertyName("OperationId");
                writer.WriteInteger(-1); // TODO [ppekrol]

                writer.WriteEndObject();
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