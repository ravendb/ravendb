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
            var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();

            var result = new QueryFields();

            if (Parsed.Where != null)
            {
                result.Where = new WhereFields();

                foreach (var whereField in GetFieldValueTokens(Parsed.Where))
                {
                    var fieldName = QueryExpression.Extract(Parsed.QueryText, whereField.Field);

                    if (whereField.SingleValue != null)
                    {
                        var singleValueToken = whereField.SingleValue;

                        if (whereField.Type == ValueTokenType.Parameter)
                            result.Where.Add(fieldName, GetParameterValueAndType(singleValueToken, ref propertyDetails));
                        else
                            result.Where.Add(fieldName, (ExtractTokenValue(singleValueToken), singleValueToken.Type));
                    }
                    else
                    {
                        var values = new List<string>(whereField.Values.Count);
                        ValueTokenType? valuesType = null;

                        foreach (var item in whereField.Values)
                        {
                            string value;
                            ValueTokenType type;

                            if (whereField.Type == ValueTokenType.Parameter)
                                (value, type) = GetParameterValueAndType(item, ref propertyDetails);
                            else
                                (value, type) = (ExtractTokenValue(item), item.Type);

                            if (valuesType != null && valuesType.Value != type && TryHandleIncompatibleTypes(ref valuesType, ref type) == false)
                            {
                                if (whereField.Type == ValueTokenType.Parameter)
                                    ThrowIncompatibleTypesInQueryParameters(fieldName, whereField.Values.Select(x => GetParameterValueAndType(x, ref propertyDetails)));
                                else
                                    ThrowIncompatibleTypesOfVariables(fieldName, whereField.Values.Select(x => (ExtractTokenValue(x), x.Type)));
                            }

                            valuesType = type;
                            values.Add(value);
                        }

                        Debug.Assert(valuesType != null);

                        result.Where.Add(fieldName, values, valuesType.Value);
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

        private string ExtractTokenValue(ValueToken singleValueToken)
        {
            string value;
            switch (singleValueToken.Type)
            {
                case ValueTokenType.String:
                    value = QueryExpression.Extract(Parsed.QueryText, singleValueToken.TokenStart + 1, singleValueToken.TokenLength - 2, singleValueToken.EscapeChars);
                    break;
                default:
                    value = QueryExpression.Extract(Parsed.QueryText, singleValueToken);
                    break;
            }
            return value;
        }

        private (string Value, ValueTokenType Type) GetParameterValueAndType(ValueToken valueToken, ref BlittableJsonReaderObject.PropertyDetails propertyDetails)
        {
            var parameterName = QueryExpression.Extract(Parsed.QueryText, valueToken);

            var index = QueryParameters.GetPropertyIndex(parameterName);

            QueryParameters.GetPropertyByIndex(index, ref propertyDetails);

            switch (propertyDetails.Token)
            {
                case BlittableJsonToken.Integer:
                    return (propertyDetails.Value.ToString(), ValueTokenType.Long);
                case BlittableJsonToken.LazyNumber:
                    return (propertyDetails.Value.ToString(), ValueTokenType.Double);
                case BlittableJsonToken.String:
                case BlittableJsonToken.CompressedString:
                    return (propertyDetails.Value.ToString(), ValueTokenType.String);
                case BlittableJsonToken.Boolean:
                    var booleanValue = (bool)propertyDetails.Value;

                    if (booleanValue)
                        return (null, ValueTokenType.True);
                    else
                        return (null, ValueTokenType.False);
                case BlittableJsonToken.Null:
                    return (null, ValueTokenType.Null);
                default:
                    throw new ArgumentException($"Unhandled token: {propertyDetails.Token}");
            }
        }

        private IEnumerable<(FieldToken Field, ValueTokenType Type, ValueToken SingleValue, List<ValueToken> Values)> GetFieldValueTokens(QueryExpression expression)
        {
            if (expression.Field != null)
            {
                switch (expression.Type)
                {
                    case OperatorType.Equal:
                    case OperatorType.LessThen:
                    case OperatorType.GreaterThen:
                    case OperatorType.LessThenEqual:
                    case OperatorType.GreaterThenEqual:
                        var value = expression.Value ?? expression.First;
                        yield return (expression.Field, value.Type, value, null);
                        yield break;
                    case OperatorType.Between:
                        // TODO arek - can queries have some values parametrized while not all of them?
                        yield return (expression.Field, expression.First.Type, null, new List<ValueToken>(2) { expression.First, expression.Second });
                        yield break;
                    case OperatorType.In:
                        // TODO arek - expression.Values[0].Type
                        yield return (expression.Field, expression.Values[0].Type, null, expression.Values);
                        yield break;
                    default:
                        throw new ArgumentException(expression.Type.ToString());
                }
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

        private static bool TryHandleIncompatibleTypes(ref ValueTokenType? valuesType, ref ValueTokenType type)
        {
            Debug.Assert(valuesType != null);

            if (valuesType.Value == ValueTokenType.Long && type == ValueTokenType.Double)
            {
                valuesType = ValueTokenType.Double;
                return true;
            }

            if (valuesType.Value == ValueTokenType.Double && type == ValueTokenType.Long)
            {
                type = ValueTokenType.Double;
                return true;
            }

            return false;
        }

        private static void ThrowIncompatibleTypesInQueryParameters(string fieldName, IEnumerable<(string Value, ValueTokenType Type)> parameters)
        {
            throw new InvalidOperationException($"Incompatible types of parameters in WHERE clause on '{fieldName}' field: " +
                                                $"{string.Join(",", parameters.Select(x => $"{x.Value}({x.Type})"))}");
        }

        private static void ThrowIncompatibleTypesOfVariables(string fieldName, IEnumerable<(string Value, ValueTokenType Type)> parameters)
        {
            throw new InvalidOperationException($"Incompatible types of variables in WHERE clause on '{fieldName}' field: " +
                                                $"{string.Join(",", parameters.Select(x => $"{x.Value}({x.Type})"))}");
        }
    }
}