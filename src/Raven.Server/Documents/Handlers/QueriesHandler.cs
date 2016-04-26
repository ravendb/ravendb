using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Indexing;
using Raven.Server.Documents.Queries;
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
            var query = GetIndexQuery(Database.Configuration.Core.MaxPageSize);

            DocumentsOperationContext context;
            using (var token = CreateTimeLimitedOperationToken())
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                var existingResultEtag = GetLongFromHeaders("If-None-Match");
                var includes = GetStringValuesQueryString("include", required: false);

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
                    writer.WriteStartObject();

                    writer.WritePropertyName(context.GetLazyString(nameof(result.IndexName)));
                    writer.WriteString(context.GetLazyString(result.IndexName));

                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(nameof(result.Results)));
                    WriteDocuments(context, writer, result.Results);

                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(result.TotalResults)));
                    writer.WriteInteger(result.TotalResults);

                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(nameof(result.Includes)));
                    WriteDocuments(context, writer, result.Includes);

                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(result.IndexTimestamp)));
                    writer.WriteString(context.GetLazyString(result.IndexTimestamp.ToString(Default.DateTimeFormatsToWrite)));

                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(result.LastQueryTime)));
                    writer.WriteString(context.GetLazyString(result.LastQueryTime.ToString(Default.DateTimeFormatsToWrite)));

                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(result.IsStale)));
                    writer.WriteBool(result.IsStale);

                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(result.ResultEtag)));
                    writer.WriteInteger(result.ResultEtag);

                    writer.WriteEndObject();
                    writer.Flush();
                }
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

        private async Task ExecuteQueryOperation(Func<QueryRunner, string, IndexQuery, QueryOperationOptions, OperationCancelToken, Task> operation, DocumentsOperationContext context)
        {
            var indexName = RouteMatch.Url.Substring(RouteMatch.MatchLength);

            var query = GetIndexQuery(int.MaxValue);
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

                writer.WritePropertyName(context.GetLazyString("OperationId"));
                writer.WriteInteger(-1); // TODO [ppekrol]

                writer.WriteEndObject();
            }
        }

        private QueryOperationOptions GetQueryOperationOptions()
        {
            return new QueryOperationOptions
            {
                AllowStale = GetBoolValueQueryString("allowStale"),
                MaxOpsPerSecond = GetIntQueryString("maxOpsPerSec", required: false),
                StaleTimeout = GetTimeSpanQueryString("staleTimeout", required: false),
                RetrieveDetails = GetBoolValueQueryString("details")
            };
        }

        private IndexQuery GetIndexQuery(int maxPageSize)
        {
            var result = new IndexQuery
            {
                // all defaults which need to have custom value
                PageSize = maxPageSize
            };

            foreach (var item in HttpContext.Request.Query)
            {
                try
                {
                    switch (item.Key)
                    {
                        case "query":
                            result.Query = item.Value[0];
                            break;
                        case StartParameter:
                            result.Start = GetStart();
                            break;
                        case PageSizeParameter:
                            result.PageSize = GetPageSize(maxPageSize);
                            break;
                        case "cutOffEtag":
                            result.CutoffEtag = long.Parse(item.Value[0]);
                            break;
                        case "waitForNonStaleResultsAsOfNow":
                            result.WaitForNonStaleResultsAsOfNow = bool.Parse(item.Value[0]);
                            break;
                        case "waitForNonStaleResultsTimeout":
                            result.WaitForNonStaleResultsTimeout = TimeSpan.Parse(item.Value[0]);
                            break;
                        case "fetch":
                            result.FieldsToFetch = item.Value;
                            break;
                        case "defaultField":
                            result.DefaultOperator = "And".Equals(item.Value[0], StringComparison.OrdinalIgnoreCase) ?
                                                            QueryOperator.And : QueryOperator.Or;
                            break;
                        case "sort":
                            result.SortedFields = item.Value.Select(y => new SortedField(y)).ToArray();
                            break;
                        case "mapReduce":
                            result.DynamicMapReduceFields = ParseDynamicMapReduceFields(item.Value); ;

                            break;
                            // TODO: HighlightedFields, HighlighterPreTags, HighlighterPostTags, HighlighterKeyName, ResultsTransformer, TransformerParameters, ExplainScores, IsDistinct
                            // TODO: AllowMultipleIndexEntriesForSameDocumentToResultTransformer, ShowTimings and spatial stuff
                            // TODO: We also need to make sure that we aren't using headers
                    }
                }
                catch (Exception e)
                {
                    throw new ArgumentException($"Could not handle query string parameter '{item.Key}' (value: {item.Value})", e);
                }
            }

            if (result.Query == null)
            {
                /* TODO arek queryFromPostRequest ?? */

                result.Query = string.Empty;
            }

            return result;
        }

        private static DynamicMapReduceField[] ParseDynamicMapReduceFields(StringValues item)
        {
            var mapReduceFields = new DynamicMapReduceField[item.Count];

            for (int i = 0; i < item.Count; i++)
            {
                var mapReduceField = item[i].Split('-');

                if (mapReduceField.Length != 3)
                    throw new InvalidOperationException($"Invalid format of dynamic map-reduce field: {item[i]}");

                FieldMapReduceOperation operation;

                if (Enum.TryParse(mapReduceField[1], out operation) == false)
                    throw new InvalidOperationException($"Could not parse map-reduce field operation: {mapReduceField[2]}");

                mapReduceFields[i] = new DynamicMapReduceField
                {
                    Name = mapReduceField[0],
                    OperationType = operation,
                    IsGroupBy = bool.Parse(mapReduceField[2]),
                };
            }
            return mapReduceFields;
        }
    }
}