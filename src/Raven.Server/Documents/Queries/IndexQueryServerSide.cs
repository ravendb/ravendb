using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Indexing;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries
{
    public class IndexQueryServerSide : IndexQuery<BlittableJsonReaderObject>
    {
        public static IndexQueryServerSide Create(HttpContext httpContext, int start, int pageSize, JsonOperationContext context)
        {
            var result = new IndexQueryServerSide
            {
                // all defaults which need to have custom value
                Start = start,
                PageSize = pageSize
            };

            DynamicJsonValue transformerParameters = null;
            HashSet<string> includes = null;
            foreach (var item in httpContext.Request.Query)
            {
                try
                {
                    switch (item.Key)
                    {
                        case "query":
                            result.Query = item.Value[0];
                            break;
                        case RequestHandler.StartParameter:
                        case RequestHandler.PageSizeParameter:
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
                            result.DynamicMapReduceFields = ParseDynamicMapReduceFields(item.Value);
                            break;
                        case "include":
                            if (includes == null)
                                includes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                            includes.Add(item.Value[0]);
                            break;
                        case "distinct":
                            result.IsDistinct = bool.Parse(item.Value[0]);
                            break;
                        case "transformer":
                            result.Transformer = item.Value[0];
                            break;
                        default:
                            const string TransformerParameterPrefix = "tp-";
                            if (item.Key.StartsWith(TransformerParameterPrefix, StringComparison.OrdinalIgnoreCase))
                            {
                                if (transformerParameters == null)
                                    transformerParameters = new DynamicJsonValue();

                                transformerParameters[item.Key.Substring(TransformerParameterPrefix.Length)] = item.Value[0];
                            }
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

            if (includes != null)
                result.Includes = includes.ToArray();

            if (transformerParameters != null)
                result.TransformerParameters = context.ReadObject(transformerParameters, "transformer/parameters");

            if (result.Query == null)
            {
                /* TODO arek queryFromPostRequest ?? */

                result.Query = string.Empty;
            }

            return result;
        }

        private static BlittableJsonReaderObject ParseTransformerParameters(StringValues item)
        {
            throw new NotImplementedException();
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