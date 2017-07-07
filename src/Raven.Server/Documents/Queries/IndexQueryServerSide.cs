using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Transformers;
using Raven.Client.Util;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries
{
    public class IndexQueryServerSide : IndexQuery<BlittableJsonReaderObject>
    {
        public SortedField[] SortedFields { get; set; }

        public string[] FieldsToFetch { get; set; }

        public QueryOperator DefaultOperator { get; set; }

        public string DefaultField { get; set; }

        public bool IsDistinct { get; set; }

        public IndexQueryServerSide()
        {
            // TODO arek - remove me
        }
        
        public IndexQueryServerSide(string query)
        {
            Query = EscapingHelper.UnescapeLongDataString(query);

            var qp = new QueryParser();
            qp.Init(query);

            Parsed = qp.Parse();
        }
        
        public Query Parsed { get; private set; }
        
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
                            result.Query = EscapingHelper.UnescapeLongDataString(item.Value[0]);

                            var qp = new QueryParser();
                            qp.Init("FROM Users WHERE Name = 'Arek'"/*result.Query*/);

                            result.Parsed = qp.Parse();
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
                        case "operator":
                            result.DefaultOperator = "And".Equals(item.Value[0], StringComparison.OrdinalIgnoreCase) ?
                                                            QueryOperator.And : QueryOperator.Or;
                            break;
                        case "defaultField":
                            result.DefaultField = item.Value;
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
                        case "skipDuplicateChecking":
                            result.SkipDuplicateChecking = bool.Parse(item.Value[0]);
                            break;
                        case "allowMultipleIndexEntriesForSameDocumentToResultTransformer":
                            result.AllowMultipleIndexEntriesForSameDocumentToResultTransformer = bool.Parse(item.Value[0]);
                            break;
                        default:
                            if (item.Key.StartsWith(TransformerParameter.Prefix, StringComparison.OrdinalIgnoreCase))
                            {
                                if (transformerParameters == null)
                                    transformerParameters = new DynamicJsonValue();

                                transformerParameters[item.Key.Substring(TransformerParameter.Prefix.Length)] = item.Value[0];
                            }
                            break;
                            // TODO: HighlightedFields, HighlighterPreTags, HighlighterPostTags, HighlighterKeyName, ExplainScores
                            // TODO: ShowTimings and spatial stuff
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
                result.Query = string.Empty;
            }

            return result;
        }

        public bool IsDynamic => Parsed.From.Index == false;
        
        public string GetCollection()
        {
            var fromToken = Parsed.From.From;
            return QueryExpression.Extract(Parsed.QueryText, fromToken);
        }

        public string GetIndex()
        {
            var fromToken = Parsed.From.From;
            return QueryExpression.Extract(Parsed.QueryText,  fromToken);
        }
        
        public IEnumerable<FieldValuePair> GetWhereFields()
        {
            if (Parsed.Where == null)
                yield break;
            
            foreach (var whereField in GetFieldValueTokens(Parsed.Where))
            {
                yield return (new FieldValuePair(QueryExpression.Extract(Parsed.QueryText, whereField.Field), QueryExpression.Extract(Parsed.QueryText, whereField.Value), whereField.Value.Type));
            }
        }

        public IEnumerable<(string Name, bool Ascending)> GetOrderByFields()
        {
            if (Parsed.OrderBy == null)
                yield break;
            
            foreach (var fieldInfo in Parsed.OrderBy)
            {
                yield return (QueryExpression.Extract(Parsed.QueryText, fieldInfo.Field), fieldInfo.Ascending);
            }
        }

        private IEnumerable<(FieldToken Field, ValueToken Value)> GetFieldValueTokens(QueryExpression expression)
        {
            if (expression.Field != null)
            {
                yield return (expression.Field, expression.Value ?? expression.First);
                yield break;
            }

            foreach (var field in GetFieldValueTokens(expression.Left))
            {
                yield return field;
            }
            
            foreach (var field in GetFieldValueTokens(expression.Right))
            {
                yield return field;
            }
        }

        private static DynamicMapReduceField[] ParseDynamicMapReduceFields(StringValues item)
        {
            var mapReduceFields = new DynamicMapReduceField[item.Count];

            for (int i = 0; i < item.Count; i++)
            {
                var mapReduceField = item[i].Split('-');

                if (mapReduceField.Length != 3)
                    throw new InvalidOperationException($"Invalid format of dynamic map-reduce field: {item[i]}");

                if (Enum.TryParse(mapReduceField[1], out FieldMapReduceOperation operation) == false)
                    throw new InvalidOperationException($"Could not parse map-reduce field operation: {mapReduceField[2]}");

                var fieldName = mapReduceField[0];

                if (operation == FieldMapReduceOperation.Count && string.Equals(fieldName, "Count", StringComparison.OrdinalIgnoreCase) == false)
                    throw new InvalidOperationException($"The only valid field name for 'Count' operation is 'Count' but was '{fieldName}'.");

                mapReduceFields[i] = new DynamicMapReduceField
                {
                    Name = fieldName,
                    OperationType = operation,
                    IsGroupBy = bool.Parse(mapReduceField[2]),
                };
            }
            return mapReduceFields;
        }
    }
}