using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using Lucene.Net.Util;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Sparrow;
using Sparrow.Json;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.Queries.Facets
{
    public static class FacetedQueryParser
    {
        public static Dictionary<string, FacetResult> Parse(JsonOperationContext context, FacetQuery query)
        {
            var results = new Dictionary<string, FacetResult>();

            foreach (var field in query.Query.Metadata.SelectFields)
            {
                if (field.IsFacet == false)
                    ThrowInvalidFieldInFacetQuery(query, field);

                var facetField = (FacetField)field;
                if (facetField.FacetSetupDocumentId != null)
                {
                    var facetSetup = query.Facets[facetField.FacetSetupDocumentId];

                    foreach (var f in ProcessFacetSetup(facetSetup))
                    {
                        var r = ProcessFacet(f.Facet, f.Ranges, query);
                        results[r.Result.Name] = r;
                    }

                    continue;
                }

                FacetBase facet;

                if (facetField.Ranges != null && facetField.Ranges.Count > 0)
                {
                    var rangeFacet = new RangeFacet();

                    foreach (var range in facetField.Ranges)
                        rangeFacet.Ranges.Add(range.GetText(query.Query));

                    facet = rangeFacet;
                }
                else
                {
                    facet = new Facet
                    {
                        FieldName = facetField.Name,
                    };
                }

                facet.DisplayFieldName = facetField.Alias;
                facet.Aggregations = facetField.Aggregations;
                facet.Options = facetField.GetOptions(context, query.Query.QueryParameters) ?? FacetOptions.Default;

                var result = ProcessFacet(facet, facetField.Ranges, query);
                results[result.Result.Name] = result;
            }

            return results;
        }

        private static IEnumerable<(FacetBase Facet, List<QueryExpression> Ranges)> ProcessFacetSetup(FacetSetup setup)
        {
            QueryParser queryParser = null;

            foreach (var f in setup.Facets)
            {
                if (f.Options == null)
                    f.Options = FacetOptions.Default;

                yield return (f, null);
            }

            foreach (var f in setup.RangeFacets)
            {
                List<QueryExpression> facetRanges = null;

                if (f.Options == null)
                    f.Options = FacetOptions.Default;

                if (f.Ranges != null && f.Ranges.Count > 0)
                {
                    if (queryParser == null)
                        queryParser = new QueryParser();

                    facetRanges = new List<QueryExpression>();

                    foreach (var range in f.Ranges)
                    {
                        queryParser.Init(range);

                        if (queryParser.Expression(out var qr) == false)
                            throw new InvalidOperationException($"Could not parse the following range expression '{range}' from facet setup document: {setup.Id}");

                        facetRanges.Add(qr);
                    }
                }

                yield return (f, facetRanges);
            }
        }

        private static FacetResult ProcessFacet(FacetBase facet, List<QueryExpression> facetRanges, FacetQuery query)
        {
            var result = new FacetResult
            {
                Result = new Raven.Client.Documents.Queries.Facets.FacetResult(),
                Options = facet.Options
            };

            string fieldName = null;

            if (facet is Facet aggregationOnlyFacet)
            {
                result.AggregateBy = aggregationOnlyFacet.FieldName ?? Constants.Documents.Querying.Facet.AllResults;
                fieldName = result.AggregateBy;
            }
            else if (facet is RangeFacet)
            {
                Debug.Assert(facetRanges != null && facetRanges.Count > 0);

                RangeType? rangeType = null;
                var ranges = new List<ParsedRange>();

                foreach (var range in facetRanges)
                {
                    var parsedRange = ParseRange(range, query, out var type);
                    if (rangeType.HasValue == false)
                        rangeType = type;
                    else if (rangeType.Value != type)
                        ThrowDifferentTypesOfRangeValues(query, rangeType.Value, type, parsedRange.Field);

                    ranges.Add(parsedRange);

                    result.Result.Values.Add(new FacetValue
                    {
                        Range = parsedRange.RangeText
                    });

                    if (fieldName == null)
                        fieldName = parsedRange.Field;
                    else
                    {
                        if (fieldName != parsedRange.Field)
                            ThrowRangeDefinedOnDifferentFields(query, fieldName, parsedRange.Field);
                    }
                }

                result.AggregateBy = fieldName;

                result.Ranges = ranges;
                result.RangeType = rangeType.Value;
            }
            else
                ThrowUnknownFacetType(facet);

            result.Result.Name = facet.DisplayFieldName ?? fieldName;

            foreach (var kvp in facet.Aggregations)
            {
                if (result.Aggregations.TryGetValue(kvp.Value, out var value) == false)
                    result.Aggregations[kvp.Value] = value = new FacetResult.Aggregation();

                switch (kvp.Key)
                {
                    case FacetAggregation.Max:
                        value.Max = true;
                        break;
                    case FacetAggregation.Min:
                        value.Min = true;
                        break;
                    case FacetAggregation.Average:
                        value.Average = true;
                        break;
                    case FacetAggregation.Sum:
                        value.Sum = true;
                        break;
                }
            }

            return result;
        }

        private static ParsedRange ParseRange(QueryExpression expression, FacetQuery query, out RangeType type)
        {
            if (expression is BetweenExpression bee)
            {
                var hValue = ConvertFieldValue(bee.Max.Token, bee.Max.Value, query.Query.QueryParameters);
                var lValue = ConvertFieldValue(bee.Min.Token, bee.Min.Value, query.Query.QueryParameters);

                var fieldName = ((FieldExpression)bee.Source).GetText(null);

                if (hValue.Type != lValue.Type)
                    ThrowDifferentTypesOfRangeValues(query, hValue.Type, lValue.Type, fieldName);

                type = hValue.Type;

                var range = new ParsedRange
                {
                    Field = fieldName,
                    HighInclusive = true,
                    HighValue = hValue.Value,
                    LowInclusive = true,
                    LowValue = lValue.Value,
                    RangeText = expression.GetText(query.Query)
                };

                return range;
            }

            if (expression is BinaryExpression be)
            {
                switch (be.Operator)
                {
                    case OperatorType.LessThan:
                    case OperatorType.GreaterThan:
                    case OperatorType.LessThanEqual:
                    case OperatorType.GreaterThanEqual:
                        var fieldName = ExtractFieldName(be, query);

                        var r = (ValueExpression)be.Right;
                        var fieldValue = ConvertFieldValue(r.Token, r.Value, query.Query.QueryParameters);

                        type = fieldValue.Type;

                        var range = new ParsedRange
                        {
                            Field = fieldName,
                            RangeText = expression.GetText(query.Query)
                        };

                        if (be.Operator == OperatorType.LessThan || be.Operator == OperatorType.LessThanEqual)
                        {
                            range.HighValue = fieldValue.Value;
                            range.HighInclusive = be.Operator == OperatorType.LessThanEqual;

                            return range;
                        }

                        if (be.Operator == OperatorType.GreaterThan || be.Operator == OperatorType.GreaterThanEqual)
                        {
                            range.LowValue = fieldValue.Value;
                            range.LowInclusive = be.Operator == OperatorType.GreaterThanEqual;

                            return range;
                        }

                        return range;
                    case OperatorType.And:
                        var left = ParseRange(be.Left, query, out var lType);
                        var right = ParseRange(be.Right, query, out var rType);

                        if (lType != rType)
                            ThrowDifferentTypesOfRangeValues(query, lType, rType, left.Field);

                        type = lType;

                        if (left.HighValue == null)
                        {
                            left.HighValue = right.HighValue;
                            left.HighInclusive = right.HighInclusive;
                        }

                        if (left.LowValue == null)
                        {
                            left.LowValue = right.LowValue;
                            left.LowInclusive = right.LowInclusive;
                        }

                        left.RangeText = $"{left.RangeText} and {right.RangeText}";
                        return left;
                    default:
                        ThrowUnsupportedRangeOperator(query, be.Operator);
                        break;
                }
            }

            ThrowUnsupportedRangeExpression(query, expression);
            type = RangeType.None;
            return null;
        }

        private static string ExtractFieldName(BinaryExpression be, FacetQuery query)
        {
            if (be.Left is FieldExpression lfe)
                return lfe.GetText(null);

            if (be.Left is ValueExpression lve)
                return lve.Token;

            ThrowUnsupportedRangeExpression(query, be.Left);
            return null;
        }

        private static (string Value, RangeType Type) ConvertFieldValue(string value, ValueTokenType type, BlittableJsonReaderObject queryParameters)
        {
            switch (type)
            {
                case ValueTokenType.Long:
                    var lng = QueryBuilder.ParseInt64WithSeparators(value);
                    return (NumericUtils.DoubleToPrefixCoded(lng), RangeType.Double);
                case ValueTokenType.Double:
                    var dbl = double.Parse(value, CultureInfo.InvariantCulture);
                    return (NumericUtils.DoubleToPrefixCoded(dbl), RangeType.Double);
                case ValueTokenType.String:
                    return (value, RangeType.None);
                case ValueTokenType.Null:
                    return (null, RangeType.None);
                case ValueTokenType.Parameter:
                    queryParameters.TryGet(value, out object o);
                    var rangeType = RangeType.None;
                    if (o is long l) 
                    {
                        o = NumericUtils.DoubleToPrefixCoded(l);
                        rangeType = RangeType.Double;
                    }
                    else if (o is LazyNumberValue lnv)
                    {
                        o = NumericUtils.DoubleToPrefixCoded((double)lnv);
                        rangeType = RangeType.Double;
                    }
                    return (o?.ToString(), rangeType);
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }

        public class ParsedRange
        {
            public bool LowInclusive;
            public bool HighInclusive;
            public string LowValue;
            public string HighValue;
            public string RangeText;
            public string Field;

            public bool IsMatch(string value)
            {
                var compareLow =
                    LowValue == null
                        ? -1
                        : string.CompareOrdinal(value, LowValue);
                var compareHigh = HighValue == null ? 1 : string.CompareOrdinal(value, HighValue);
                // if we are range exclusive on either end, check that we will skip the edge values
                if (compareLow == 0 && LowInclusive == false ||
                    compareHigh == 0 && HighInclusive == false)
                    return false;

                if (LowValue != null && compareLow < 0)
                    return false;

                if (HighValue != null && compareHigh > 0)
                    return false;

                return true;
            }

            public override string ToString()
            {
                return string.Format("{0}:{1}", Field, RangeText);
            }
        }

        public class FacetResult
        {
            public FacetResult()
            {
                Aggregations = new Dictionary<string, Aggregation>(StringComparer.OrdinalIgnoreCase);
            }

            public string AggregateBy;

            public Dictionary<string, Aggregation> Aggregations;

            public RangeType RangeType;

            public List<ParsedRange> Ranges;

            public Raven.Client.Documents.Queries.Facets.FacetResult Result;

            public FacetOptions Options;

            public class Aggregation
            {
                public bool Sum;

                public bool Min;

                public bool Max;

                public bool Average;
            }
        }

        private static void ThrowDifferentTypesOfRangeValues(FacetQuery query, RangeType type1, RangeType type2, string field)
        {
            throw new InvalidQueryException(
                $"Expected to get values of the same type in range expressions while it got {type1} and {type2} for a field '{field}'",
                query.Query.Metadata.QueryText, query.Query.QueryParameters);
        }

        private static void ThrowUnknownFacetType(FacetBase facet)
        {
            throw new InvalidOperationException($"Unsupported facet type: {facet.GetType().FullName}");
        }

        private static void ThrowRangeDefinedOnDifferentFields(FacetQuery query, string fieldName, string differentField)
        {
            throw new InvalidQueryException($"Facet ranges must be defined on the same field while we got '{fieldName}' and '{differentField}' used in the same faced",
                query.Query.Metadata.QueryText, query.Query.QueryParameters);
        }

        private static void ThrowInvalidFieldInFacetQuery(FacetQuery query, SelectField field)
        {
            throw new InvalidQueryException(
                $"It encountered a field in SELECT clause which is not a facet. Field: {field.Name}", query.Query.Metadata.QueryText,
                query.Query.QueryParameters);
        }

        private static void ThrowUnsupportedRangeOperator(FacetQuery query, OperatorType op)
        {
            throw new InvalidQueryException($"Unsupported operator in a range of a facet query: {op}", query.Query.Metadata.QueryText, query.Query.QueryParameters);
        }

        private static void ThrowUnsupportedRangeExpression(FacetQuery query, QueryExpression expression)
        {
            throw new InvalidQueryException($"Unsupported range expression of a facet query: {expression.GetType().Name}. Text: {expression.GetText(query.Query)}.", query.Query.Metadata.QueryText, query.Query.QueryParameters);
        }
    }
}

