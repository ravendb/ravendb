using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Http;
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
        private string _indexName;

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

        public static IndexQueryServerSide Create(BlittableJsonReaderObject json)
        {
            if (json.TryGet(nameof(Query), out string query) == false || string.IsNullOrEmpty(query))
                throw new InvalidOperationException($"Index query does not contain '{nameof(Query)}' field.");

            var result = new IndexQueryServerSide(query);

            result.AllowMultipleIndexEntriesForSameDocumentToResultTransformer = json.GetWithoutThrowingOnError<bool>(nameof(result.AllowMultipleIndexEntriesForSameDocumentToResultTransformer));
            result.CutoffEtag = json.GetWithoutThrowingOnError<long?>(nameof(result.CutoffEtag));
            result.DisableCaching = json.GetWithoutThrowingOnError<bool>(nameof(result.DisableCaching));
            result.ExplainScores = json.GetWithoutThrowingOnError<bool>(nameof(result.ExplainScores));
            result.PageSize = json.GetWithoutThrowingOnError<int>(nameof(result.PageSize));
            result.Start = json.GetWithoutThrowingOnError<int>(nameof(result.Start));
            result.ShowTimings = json.GetWithoutThrowingOnError<bool>(nameof(result.ShowTimings));
            result.SkipDuplicateChecking = json.GetWithoutThrowingOnError<bool>(nameof(result.SkipDuplicateChecking));
            result.Transformer = json.GetWithoutThrowingOnError<string>(nameof(result.Transformer));
            result.WaitForNonStaleResultsTimeout = json.GetWithoutThrowingOnError<TimeSpan?>(nameof(result.WaitForNonStaleResultsTimeout));
            result.WaitForNonStaleResults = json.GetWithoutThrowingOnError<bool>(nameof(result.WaitForNonStaleResults));
            result.WaitForNonStaleResultsAsOfNow = json.GetWithoutThrowingOnError<bool>(nameof(result.WaitForNonStaleResultsAsOfNow));

            if (json.TryGet(nameof(result.Includes), out BlittableJsonReaderArray includesArray) && includesArray != null && includesArray.Length > 0)
            {
                result.Includes = new string[includesArray.Length];
                for (var i = 0; i < includesArray.Length; i++)
                    result.Includes[i] = includesArray.GetStringByIndex(i);
            }

            if (json.TryGet(nameof(result.QueryParameters), out BlittableJsonReaderObject qp))
                result.QueryParameters = qp;

            if (json.TryGet(nameof(result.TransformerParameters), out BlittableJsonReaderObject tp))
                result.TransformerParameters = tp;

            return result;
        }

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
                            qp.Init(result.Query);

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
            if (_indexName == null)
            {
                var fromToken = Parsed.From.From;
                _indexName = QueryExpression.Extract(Parsed.QueryText, fromToken.TokenStart + 1, fromToken.TokenLength - 2, fromToken.EscapeChars);
            }

            return _indexName;
        }

        private QueryFields _fields;

        public QueryFields Fields
        {
            get
            {
                if (_fields != null)
                    return _fields;

                return _fields = RetrieveFields();
            }
        }

        private QueryFields RetrieveFields()
        {
            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

            var result = new QueryFields();

            if (Parsed.Where != null)
            {
                result.Where = new WhereFields();

                foreach (var whereField in GetFieldValueTokens(Parsed.Where))
                {
                    var fieldName = QueryExpression.Extract(Parsed.QueryText, whereField.Field);

                    if (whereField.Value.Type == ValueTokenType.Parameter)
                    {
                        var parameterName = QueryExpression.Extract(Parsed.QueryText, whereField.Value);

                        var index = QueryParameters.GetPropertyIndex(parameterName);

                        QueryParameters.GetPropertyByIndex(index, ref propertyDetails);

                        string value = null;
                        ValueTokenType type;

                        switch (propertyDetails.Token)
                        {
                            case BlittableJsonToken.Integer:
                                value = propertyDetails.Value.ToString();
                                type = ValueTokenType.Long;
                                break;
                            case BlittableJsonToken.LazyNumber:
                                value = propertyDetails.Value.ToString();
                                type = ValueTokenType.Double;
                                break;
                            case BlittableJsonToken.String:
                            case BlittableJsonToken.CompressedString:
                                value = propertyDetails.Value.ToString();
                                type = ValueTokenType.String;
                                break;
                            case BlittableJsonToken.Boolean:
                                var booleanValue = (bool)propertyDetails.Value;

                                if (booleanValue)
                                    type = ValueTokenType.True;
                                else
                                    type = ValueTokenType.False;
                                break;
                            case BlittableJsonToken.Null:
                                type = ValueTokenType.Null;
                                break;
                            default:
                                throw new ArgumentException($"Unhandled token: {propertyDetails.Token}");
                        }

                        result.Where.Add(fieldName, value, type);
                    }
                    else
                    {
                        string value;
                        switch (whereField.Value.Type)
                        {
                            case ValueTokenType.String:
                                value = QueryExpression.Extract(Parsed.QueryText, whereField.Value.TokenStart + 1, whereField.Value.TokenLength - 2, whereField.Value.EscapeChars);
                                break;
                            default:
                                value = QueryExpression.Extract(Parsed.QueryText, whereField.Value);
                                break;
                        }

                        result.Where.Add(fieldName, value, whereField.Value.Type);
                    }
                }
            }

            if (Parsed.OrderBy != null)
            {
                result.OrderBy = new List<(string Name, OrderByFieldType OrderingType, bool Ascending)>();

                foreach (var fieldInfo in Parsed.OrderBy)
                {
                    result.OrderBy.Add((QueryExpression.Extract(Parsed.QueryText, fieldInfo.Field), fieldInfo.FieldType, fieldInfo.Ascending));
                }
            }

            return result;
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
    }
}