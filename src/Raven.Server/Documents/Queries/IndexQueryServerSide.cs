using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            if (json.TryGet(nameof(Query), out string query) == false || string.IsNullOrWhiteSpace(query))
                throw new InvalidOperationException($"Index query does not contain '{nameof(Query)}' field.");

            var result = new IndexQueryServerSide(query);

            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
            foreach (var propertyIndex in json.GetPropertiesByInsertionOrder())
            {
                json.GetPropertyByIndex(propertyIndex, ref propertyDetails);

                switch (propertyDetails.Name)
                {
                    case nameof(Query):
                        continue;
                    case nameof(AllowMultipleIndexEntriesForSameDocumentToResultTransformer):
                        result.AllowMultipleIndexEntriesForSameDocumentToResultTransformer = (bool)propertyDetails.Value;
                        break;
                    case nameof(CutoffEtag):
                        result.CutoffEtag = (long?)propertyDetails.Value;
                        break;
                    case nameof(DisableCaching):
                        result.DisableCaching = (bool)propertyDetails.Value;
                        break;
                    case nameof(ExplainScores):
                        result.ExplainScores = (bool)propertyDetails.Value;
                        break;
                    case nameof(PageSize):
                        result.PageSize = (int)(long)propertyDetails.Value;
                        break;
                    case nameof(Start):
                        result.Start = (int)(long)propertyDetails.Value;
                        break;
                    case nameof(ShowTimings):
                        result.ShowTimings = (bool)propertyDetails.Value;
                        break;
                    case nameof(SkipDuplicateChecking):
                        result.SkipDuplicateChecking = (bool)propertyDetails.Value;
                        break;
                    case nameof(Transformer):
                        result.Transformer = propertyDetails.Value?.ToString();
                        break;
                    case nameof(WaitForNonStaleResultsTimeout):
                        if (propertyDetails.Value != null)
                            result.WaitForNonStaleResultsTimeout = TimeSpan.Parse(propertyDetails.Value.ToString());
                        break;
                    case nameof(WaitForNonStaleResults):
                        result.WaitForNonStaleResults = (bool)propertyDetails.Value;
                        break;
                    case nameof(WaitForNonStaleResultsAsOfNow):
                        result.WaitForNonStaleResultsAsOfNow = (bool)propertyDetails.Value;
                        break;
                    case nameof(Includes):
                        var includesArray = propertyDetails.Value as BlittableJsonReaderArray;
                        if (includesArray == null || includesArray.Length == 0)
                            continue;

                        result.Includes = new string[includesArray.Length];
                        for (var i = 0; i < includesArray.Length; i++)
                            result.Includes[i] = includesArray.GetStringByIndex(i);
                        break;
                    case nameof(QueryParameters):
                        result.QueryParameters = (BlittableJsonReaderObject)propertyDetails.Value;
                        break;
                    case nameof(TransformerParameters):
                        result.TransformerParameters = (BlittableJsonReaderObject)propertyDetails.Value;
                        break;
                }
            }

            return result;
        }

        public static IndexQueryServerSide Create(HttpContext httpContext, int start, int pageSize, JsonOperationContext context)
        {
            if (httpContext.Request.Query.TryGetValue("query", out var query) == false || query.Count == 0 || string.IsNullOrWhiteSpace(query[0]))
                throw new InvalidOperationException("Missing mandatory query string parameter 'query'.");

            var result = new IndexQueryServerSide(EscapingHelper.UnescapeLongDataString(query[0]))
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
                            continue;
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
            var result = new QueryFields();

            if (Parsed.Where != null)
            {
                result.Where = new WhereFields();

                foreach (var whereField in GetWhereTokens(Parsed.Where))
                {
                    if (whereField.SingleValue != null)
                    {
                        var valueAndType = whereField.GetSingleValueAndType(Parsed.QueryText, QueryParameters);

                        result.Where.Add(whereField.ExtractFieldName(Parsed.QueryText), valueAndType);
                    }
                    else
                    {
                        var (values, type) = whereField.GetValuesAndType(Parsed.QueryText, QueryParameters);

                        result.Where.Add(whereField.ExtractFieldName(Parsed.QueryText), values, type);
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

        private IEnumerable<WhereFieldTokens> GetWhereTokens(QueryExpression whereExpression)
        {
            if (whereExpression.Field == null)
            {
                foreach (var field in GetWhereTokens(whereExpression.Left))
                {
                    yield return field;
                }

                foreach (var field in GetWhereTokens(whereExpression.Right))
                {
                    yield return field;
                }
            }

            switch (whereExpression.Type)
            {
                case OperatorType.Method:
                    throw new NotImplementedException();
                default:
                    foreach (var fieldValueToken in GetFieldValueTokens(whereExpression))
                    {
                        yield return fieldValueToken;
                    }
                    yield break;
            }
        }

        private IEnumerable<WhereFieldTokens> GetFieldValueTokens(QueryExpression expression)
        {
            Debug.Assert(expression.Field != null);

            switch (expression.Type)
            {
                case OperatorType.Equal:
                case OperatorType.LessThen:
                case OperatorType.GreaterThen:
                case OperatorType.LessThenEqual:
                case OperatorType.GreaterThenEqual:
                    var value = expression.Value ?? expression.First;
                    yield return new WhereFieldTokens(expression.Field, value.Type, value, null, expression.Type);
                    yield break;
                case OperatorType.Between:
                    if (expression.First.Type != expression.Second.Type)
                        ThrowIncompatibleTypesOfVariables(expression.Field, expression.First, expression.Second);

                    yield return new WhereFieldTokens(expression.Field, expression.First.Type, null, new List<ValueToken>(2) { expression.First, expression.Second }, expression.Type);
                    yield break;
                case OperatorType.In:
                    for (int i = 0; i < expression.Values.Count - 1; i++)
                    {
                        if (expression.Values[i] != expression.Values[i + 1])
                            ThrowIncompatibleTypesOfVariables(expression.Field, expression.Values.ToArray());
                    }

                    yield return new WhereFieldTokens(expression.Field, expression.Values[0].Type, null, expression.Values, expression.Type);
                    yield break;
                default:
                    throw new ArgumentException(expression.Type.ToString());
            }
        }

        //private static void ThrowIncompatibleTypesInQueryParameters(string fieldName, IEnumerable<(string Value, ValueTokenType Type)> parameters)
        //{
        //    throw new InvalidOperationException($"Incompatible types of parameters in WHERE clause on '{fieldName}' field: " +
        //                                        $"{string.Join(",", parameters.Select(x => $"{x.Value}({x.Type})"))}");
        //}

        //private static void ThrowIncompatibleTypesOfVariables(string fieldName, IEnumerable<ValueToken> parameters)
        //{
        //    throw new InvalidOperationException($"Incompatible types of variables in WHERE clause on '{fieldName}' field: " +
        //                                        $"{string.Join(",", parameters.Select(x => $"{x.Value}({x.Type})"))}");
        //}

        private void ThrowIncompatibleTypesOfVariables(FieldToken fieldName, params ValueToken[] valueTokens)
        {
            throw new InvalidOperationException($"Incompatible types of variables in WHERE clause");
            //TODO arek
            //throw new InvalidOperationException($"Incompatible types of variables in WHERE clause on '{ExtractFieldName(fieldName)}' field: " +
            //                                    $"{string.Join(",", valueTokens.Select(x => $"{ExtractTokenValue(x)}({x.Type})"))}");
        }
    }
}