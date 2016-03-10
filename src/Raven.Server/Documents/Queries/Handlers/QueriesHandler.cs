using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries.Handlers
{
    public class QueriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/indexes/$", "GET")]
        public Task Get()
        {
            var indexName = RouteMatch.Url.Substring(RouteMatch.MatchLength);
            var query = GetIndexQuery();

            //TODO arek - cancellation token

            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                var runner = new QueryRunner(IndexStore, context);

                var result = runner.ExecuteQuery(indexName, query);

                HttpContext.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
                HttpContext.Response.Headers["ETag"] = "1"; // TODO arek

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("Results"));
                    WriteDocuments(context, writer, result.Results);

                    writer.WriteComma();

                    //writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("Includes"));
                    //WriteDocuments(context, writer, documents, ids.Count, documents.Count - ids.Count);

                    //writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString("IsStale"));
                    writer.WriteBool(result.IsStale);

                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString("TotalResults"));
                    writer.WriteInteger(result.TotalResults);

                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString("IndexName"));
                    writer.WriteString(context.GetLazyString(result.IndexName));

                    writer.WriteEndObject();
                    writer.Flush();
                }

                return Task.CompletedTask;
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
                        case "cutOff":
                            result.Cutoff = DateTime.Parse(item.Value[0]);
                            break;
                        case "cutOffEtag":
                            result.CutoffEtag = long.Parse(item.Value[0]);
                            break;
                        case "waitForNonStaleResultsAsOfNow":
                            result.WaitForNonStaleResultsAsOfNow = bool.Parse(item.Value[0]);

                            if (result.WaitForNonStaleResultsAsOfNow)
                                result.Cutoff = SystemTime.UtcNow;
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
                        // TODO arek: SortHints - RavenDB-4371

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