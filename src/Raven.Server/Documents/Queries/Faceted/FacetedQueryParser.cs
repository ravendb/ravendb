using System;
using System.Collections.Generic;
using System.Globalization;
using Lucene.Net.Util;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;

namespace Raven.Server.Documents.Queries.Faceted
{
    public static class FacetedQueryParser
    {
        public static Dictionary<string, FacetResult> Parse(FacetQuery query)
        {
            var results = new Dictionary<string, FacetResult>();

            foreach (var field in query.Query.Metadata.SelectFields)
            {
                if (field.IsFacet == false)
                    throw new InvalidOperationException("Should not happen!");

                var facetField = (FacetField)field;
                if (facetField.FacetSetupDocumentId != null)
                {
                    var facetSetup = query.Facets[facetField.FacetSetupDocumentId];

                    foreach (var f in ProcessFacetSetup(facetSetup))
                    {
                        var r = ProcessFacet(f.Facet, f.Ranges);
                        results[r.Result.Name] = r;
                    }

                    continue;
                }

                var facet = new Facet
                {
                    Name = facetField.Name,
                    DisplayName = facetField.Alias,
                    Aggregations = facetField.Aggregations,
                    Options = FacetOptions.Default // TODO [ppekrol]
                };

                foreach (var range in facetField.Ranges)
                    facet.Ranges.Add(range.GetText());

                var result = ProcessFacet(facet, facetField.Ranges);
                results[result.Result.Name] = result;
            }

            return results;
        }

        private static IEnumerable<(Facet Facet, List<QueryExpression> Ranges)> ProcessFacetSetup(FacetSetup setup)
        {
            QueryParser queryParser = null;

            foreach (var f in setup.Facets)
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
                            throw new InvalidOperationException("TODO ppekrol");

                        facetRanges.Add(qr);
                    }
                }

                yield return (f, facetRanges);
            }
        }

        private static FacetResult ProcessFacet(Facet facet, List<QueryExpression> facetRanges)
        {
            var result = new FacetResult
            {
                AggregateBy = facet.Name,
                Result = new Client.Documents.Commands.FacetResult
                {
                    Name = facet.DisplayName
                },
                Options = facet.Options
            };

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

            if (facetRanges != null && facetRanges.Count > 0)
            {
                RangeType? rangeType = null;
                var ranges = new List<ParsedRange>();
                foreach (var range in facetRanges)
                {
                    var parsedRange = ParseRange(facet.Name, range, out var type);
                    if (rangeType.HasValue == false)
                        rangeType = type;
                    else if (rangeType.Value != type)
                        throw new InvalidOperationException("TODO ppekrol");

                    ranges.Add(parsedRange);

                    result.Result.Values.Add(new FacetValue
                    {
                        Range = parsedRange.RangeText
                    });
                }

                result.Ranges = ranges;
                result.RangeType = rangeType.Value;
            }

            return result;
        }

        private static ParsedRange ParseRange(string field, QueryExpression expression, out RangeType type)
        {
            if (expression is BetweenExpression bee)
            {
                var hValue = ConvertFieldValue(bee.Max.Token, bee.Max.Value);
                var lValue = ConvertFieldValue(bee.Min.Token, bee.Min.Value);

                if (hValue.Type != lValue.Type)
                    throw new InvalidOperationException("TODO ppekrol");

                type = hValue.Type;

                return new ParsedRange
                {
                    Field = field,
                    HighInclusive = true,
                    HighValue = hValue.Value,
                    LowInclusive = true,
                    LowValue = lValue.Value,
                    RangeText = expression.GetText()
                };
            }

            if (expression is BinaryExpression be)
            {
                switch (be.Operator)
                {
                    case OperatorType.LessThan:
                    case OperatorType.GreaterThan:
                    case OperatorType.LessThanEqual:
                    case OperatorType.GreaterThanEqual:
                        var fieldName = ExtractFieldName(be);

                        var r = (ValueExpression)be.Right;
                        var fieldValue = ConvertFieldValue(r.Token, r.Value);

                        type = fieldValue.Type;

                        var range = new ParsedRange
                        {
                            Field = fieldName,
                            RangeText = expression.GetText()
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
                        var left = ParseRange(field, be.Left, out var lType);
                        var right = ParseRange(field, be.Right, out var rType);

                        if (lType != rType)
                            throw new InvalidOperationException("TODO ppekrol");

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

                        left.RangeText = expression.GetText();
                        return left;
                    default:
                        throw new InvalidOperationException("TODO ppekrol");
                }
            }

            throw new InvalidOperationException("TODO ppekrol");
        }

        private static string ExtractFieldName(BinaryExpression be)
        {
            if (be.Left is FieldExpression lfe)
                return lfe.FieldValue;

            if (be.Left is ValueExpression lve)
                return lve.Token;

            throw new InvalidOperationException("TODO ppekrol");
        }

        private static (string Value, RangeType Type) ConvertFieldValue(string value, ValueTokenType type)
        {
            switch (type)
            {
                case ValueTokenType.Long:
                case ValueTokenType.Double:
                    var dbl = double.Parse(value, CultureInfo.InvariantCulture);
                    return (NumericUtils.DoubleToPrefixCoded(dbl), RangeType.Double);
                case ValueTokenType.String:
                    return (value, RangeType.None);
                case ValueTokenType.Null:
                    return (null, RangeType.None);
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

            public Raven.Client.Documents.Commands.FacetResult Result;

            public FacetOptions Options;

            public class Aggregation
            {
                public bool Sum;

                public bool Min;

                public bool Max;

                public bool Average;
            }
        }
    }
}

