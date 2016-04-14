using System;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Data;
using Raven.Server.Documents.Queries;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class QueriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/indexes/$", "GET")]
        public async Task Get()
        {
            var indexName = RouteMatch.Url.Substring(RouteMatch.MatchLength);
            var query = GetIndexQuery();

            DocumentsOperationContext context;
            using (var token = CreateTimeLimitedOperationToken())
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                var existingResultEtag = GetLongFromHeaders("If-None-Match");
                var includes = GetStringValuesQueryString("include", required: false);

                var runner = new QueryRunner(IndexStore, Database.DocumentsStorage, context);
                
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

        private IndexQuery GetIndexQuery()
        {
            var result = new IndexQuery
            {
                // all defaults which need to have custom value
                PageSize = Database.Configuration.Core.MaxPageSize
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
                            result.Start = int.Parse(item.Value[0]);
                            break;
                        case PageSizeParameter:
                            result.PageSize = int.Parse(item.Value[0]);
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
                        case "groupBy":
                            result.GroupByFields = item.Value;
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
    }
}