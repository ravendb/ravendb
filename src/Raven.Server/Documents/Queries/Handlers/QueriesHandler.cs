using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Primitives;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;

namespace Raven.Server.Documents.Queries.Handlers
{
    public class QueriesHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/indexes/$", "GET")]
        public Task Get()
        {
            var indexName = RouteMatch.Url.Substring(RouteMatch.MatchLength);
            var query = IndexQueryBuilder.Build(this);

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

                    //writer.WriteComma();
                    //writer.WritePropertyName(context.GetLazyStringForFieldWithCaching("Includes"));

                    //WriteDocuments(context, writer, documents, ids.Count, documents.Count - ids.Count);

                    writer.WriteEndObject();
                    writer.Flush();
                }

                return Task.CompletedTask;
            }
        }

        private Dictionary<string, SortOptions> GetSortHints(string sortHintPrefix)
        {
            var result = new Dictionary<string, SortOptions>();
            
            foreach (var pair in HttpContext.Request.Query.Where(pair => pair.Key.StartsWith(sortHintPrefix, StringComparison.OrdinalIgnoreCase)))
            {
                var key = pair.Key;
                var value = Uri.UnescapeDataString(pair.Value);

                SortOptions sort;
                Enum.TryParse(value, true, out sort);
                result[Uri.UnescapeDataString(key)] = sort;
            }

            return result;
        }

        private static readonly QueryStringMapping<IndexQuery, QueriesHandler> IndexQueryBuilder = new QueryStringMapping<IndexQuery, QueriesHandler>
        {
            { "query", (x, param, handler) => x.Query = handler.GetStringQueryString(param) ?? /* TODO arek queryFromPostRequest ?? */ string.Empty },
            { StartParameter, (x, _, handler) => x.Start = handler.GetStart() },
            { PageSizeParameter, (x, _, handler) => x.PageSize = handler.GetPageSize(handler.Database.Configuration.Core.MaxPageSize) },
            { "cutOff", (x, param, handler) => x.Cutoff = handler.GetDateTimeQueryString(param) },
            { "cutOffEtag", (x, param, handler) => x.CutoffEtag = handler.HttpContext.Request.Query.ContainsKey(param) ? handler.GetLongQueryString(param) : (long?) null },
            { "waitForNonStaleResultsAsOfNow", (x, param, handler) =>
                                                {
                                                    x.WaitForNonStaleResultsAsOfNow = handler.GetBoolValueQueryString(param, DefaultValue<bool>.Default);
                                                    if (x.WaitForNonStaleResultsAsOfNow)
                                                        x.Cutoff = SystemTime.UtcNow;
                                                }
            },
            { "fetch", (x, param, handler) => x.FieldsToFetch = handler.GetStringValuesQueryString(param, DefaultValue<StringValues>.Default) },
            { "defaultField", (x, param, handler) => x.DefaultField = handler.GetStringQueryString(param, DefaultValue<string>.Default) },
            { "operator", (x, param, handler) => x.DefaultOperator =
                                                    "And".Equals(handler.GetStringQueryString(param, DefaultValue<string>.Default), StringComparison.OrdinalIgnoreCase) ?
                                                        QueryOperator.And : QueryOperator.Or
            },
            { "sort", (x, param, handler) =>
                        {
                            var sortedFields = handler.GetStringValuesQueryString(param, DefaultValue<StringValues>.Default);
                            
                            if (sortedFields.Count > 0)
                                x.SortedFields = sortedFields.Select(y => new SortedField(y)).ToArray();
                            else
                                x.SortedFields = Enumerable.Empty<SortedField>().ToArray();
                        }
            },
            { "SortHint-", (x, param, handler) => x.SortHints = handler.GetSortHints(param) }
            // TODO: HighlightedFields, HighlighterPreTags, HighlighterPostTags, HighlighterKeyName, ResultsTransformer, TransformerParameters, ExplainScores, IsDistinct
            // TODO: AllowMultipleIndexEntriesForSameDocumentToResultTransformer, ShowTimings and spatial stuff
            // TODO: We also need to make sure that we aren't using headers
        };
    }
}