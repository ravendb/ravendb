using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying;
using Corax.Querying.Matches.Meta;
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
        public static Dictionary<string, FacetResult> Parse(JsonOperationContext context, FacetQuery query, SearchEngineType searchEngineType)
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
                        var r = ProcessFacet(f.Facet, f.Ranges, query, searchEngineType);
                        results[r.Result.Name] = r;
                    }

                    continue;
                }

                FacetBase facet;
                var options = facetField.GetOptions(context, query.Query.QueryParameters) ?? FacetOptions.Default;

                if (facetField.Ranges != null && facetField.Ranges.Count > 0)
                {
                    var rangeFacet = new RangeFacet();
                    
                    if (options != FacetOptions.Default)
                        ThrowWhenOptionsAreUsedForRangeFacet();

                    foreach (var range in facetField.Ranges)
                        rangeFacet.Ranges.Add(range.GetText(query.Query));

                    facet = rangeFacet;
                }
                else
                {
                    facet = new Facet
                    {
                        FieldName = facetField.Name,
                        Options = options
                    };
                }
                
                facet.DisplayFieldName = facetField.Alias;
                facet.Aggregations = facetField.Aggregations;
                
                var result = ProcessFacet(facet, facetField.Ranges, query, searchEngineType);
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

        private static FacetResult ProcessFacet(FacetBase facet, List<QueryExpression> facetRanges, FacetQuery query, SearchEngineType searchEngineType)
        {
            var result = new FacetResult
            {
                Result = new Raven.Client.Documents.Queries.Facets.FacetResult(),
                Options = facet is Facet facetWithOptions ? facetWithOptions.Options : null
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
                    var parsedRange = ParseRange(range, query, searchEngineType, out var type);
                    if (rangeType.HasValue == false)
                        rangeType = type;
                    else if (rangeType.Value != type)
                        ThrowDifferentTypesOfRangeValues(query, rangeType.Value, type, parsedRange.Field);

                    ranges.Add(parsedRange);

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
                if (query.Legacy && kvp.Value.Count > 1)
                    throw new InvalidQueryException($"Detected duplicate facet aggregation operation '{kvp.Key}'. Each facet can only contain one of each available operations.");

                foreach (var v in kvp.Value)
                {
                    if (result.Aggregations.TryGetValue(v, out var value) == false)
                        result.Aggregations[v] = value = new FacetResult.Aggregation();

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
            }

            return result;
        }

        private static ParsedRange ParseRange(QueryExpression expression, FacetQuery query, SearchEngineType searchEngineType, out RangeType type)
        {
            if (expression is BetweenExpression bee)
            {
                var hValue = searchEngineType == SearchEngineType.Corax 
                    ? ConvertFieldValueForCorax(bee.Max.Token.Value, bee.Max.Value, query.Query.QueryParameters) 
                    : ConvertFieldValueForLucene(bee.Max.Token.Value, bee.Max.Value, query.Query.QueryParameters);
                var lValue = searchEngineType == SearchEngineType.Corax 
                    ? ConvertFieldValueForCorax(bee.Min.Token.Value, bee.Min.Value, query.Query.QueryParameters)
                    : ConvertFieldValueForLucene(bee.Min.Token.Value, bee.Min.Value, query.Query.QueryParameters);

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

                if (searchEngineType is SearchEngineType.Corax)
                    return new CoraxParsedRange(range);
                
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
                        var fieldValue = searchEngineType == SearchEngineType.Corax 
                            ? ConvertFieldValueForCorax(r.Token.Value, r.Value, query.Query.QueryParameters)
                            : ConvertFieldValueForLucene(r.Token.Value, r.Value, query.Query.QueryParameters);

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

                            return GetForSearchEngine(range);
                        }

                        if (be.Operator == OperatorType.GreaterThan || be.Operator == OperatorType.GreaterThanEqual)
                        {
                            range.LowValue = fieldValue.Value;
                            range.LowInclusive = be.Operator == OperatorType.GreaterThanEqual;

                            return GetForSearchEngine(range);
                        }

                        return range;
                    case OperatorType.And:
                        var left = ParseRange(be.Left, query, searchEngineType, out var lType);
                        var right = ParseRange(be.Right, query, searchEngineType, out var rType);

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
                        return GetForSearchEngine(left);
                    default:
                        ThrowUnsupportedRangeOperator(query, be.Operator);
                        break;
                }
            }

            ThrowUnsupportedRangeExpression(query, expression);
            type = RangeType.None;
            return null;

            ParsedRange GetForSearchEngine(ParsedRange inner)
            {
                if (searchEngineType is SearchEngineType.Corax)
                    return new CoraxParsedRange(inner);
                return inner;
            }
        }

        private static string ExtractFieldName(BinaryExpression be, FacetQuery query)
        {
            if (be.Left is FieldExpression lfe)
                return lfe.GetText(null);

            if (be.Left is ValueExpression lve)
                return lve.Token.Value;

            ThrowUnsupportedRangeExpression(query, be.Left);
            return null;
        }

        private static (string Value, RangeType Type) ConvertFieldValueForLucene(string value, ValueTokenType type, BlittableJsonReaderObject queryParameters)
        {
            switch (type)
            {
                case ValueTokenType.Long:
                    var lng = QueryBuilderHelper.ParseInt64WithSeparators(value);
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

        private static (string Value, RangeType Type) ConvertFieldValueForCorax(string value, ValueTokenType type, BlittableJsonReaderObject queryParameters)
        {
            switch (type)
            {
                case ValueTokenType.Long:
                    return (QueryBuilderHelper.ParseInt64WithSeparators(value).ToString(CultureInfo.InvariantCulture), RangeType.Long);
                case ValueTokenType.Double:
                    var dbl = double.Parse(value, CultureInfo.InvariantCulture);
                    return (dbl.ToString(CultureInfo.InvariantCulture), RangeType.Double);
                case ValueTokenType.String:
                    return (value, RangeType.None);
                case ValueTokenType.Null:
                    return (null, RangeType.None);
                case ValueTokenType.Parameter:
                    queryParameters.TryGet(value, out object o);
                    var rangeType = RangeType.None;
                    if (o is long l)
                    {
                        rangeType = RangeType.Long;
                    }
                    else if (o is LazyNumberValue lnv)
                    {
                        rangeType = RangeType.Double;
                    }
                    return (o.ToString(), rangeType);
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }
        
        public sealed class CoraxParsedRange : ParsedRange
        {
            private Operation _leftSide; 
            private Operation _rightSide;
            private bool IsNumerical;
            
            
            public double LowValueAsDouble = double.MinValue;
            public double HighValueAsDouble = double.MaxValue;
            public long LowValueAsLong = long.MinValue;
            public long HighValueAsLong = long.MaxValue;
            public ReadOnlySpan<byte> HighValueAsBytes => _highValueAsBytes.AsSpan();
            private readonly byte[] _highValueAsBytes;
            
            public ReadOnlySpan<byte> LowValueAsBytes => _lowValueAsBytes.AsSpan();
            private readonly byte[] _lowValueAsBytes;
            public CoraxParsedRange(ParsedRange range)
            {
                IsNumerical = true;
                
                _leftSide = Operation.None;
                _rightSide = Operation.None;
                //Deep copy
                LowInclusive = range.LowInclusive;
                if (LowInclusive)
                    _leftSide |= Operation.Equal;
                
                HighInclusive = range.HighInclusive;
                if (HighInclusive)
                    _rightSide |= Operation.Equal;
                
                LowValue = range.LowValue;
                HighValue = range.HighValue;
                RangeText = range.RangeText;
                Field = range.Field;

                if (LowValue != null)
                {
                    _leftSide |= Operation.GreaterThan;
                    _lowValueAsBytes = Encodings.Utf8.GetBytes(LowValue);
                    long.TryParse(LowValue, out LowValueAsLong);
                    IsNumerical &= double.TryParse(LowValue, out LowValueAsDouble);
                }
                
                if (HighValue != null)
                {
                    _rightSide |= Operation.LowerThan;
                    _highValueAsBytes = Encodings.Utf8.GetBytes(HighValue);
                    long.TryParse(HighValue, out HighValueAsLong);
                    IsNumerical &= double.TryParse(HighValue, out HighValueAsDouble);
                }
            }

            public IAggregationProvider GetAggregation(IndexSearcher searcher, in FieldMetadata metadata, bool forward = true)
            {
                var type = IsNumerical ? RangeType.Double : RangeType.None;
                var lowValueRange = RangeTypeToCoraxRange(_leftSide);
                var highValueRange = RangeTypeToCoraxRange(_rightSide);
                
                //Between
                if (LowValue != null && HighValue != null)
                {
                    return type switch
                    {
                        RangeType.Double or RangeType.Long => searcher.BetweenAggregation(metadata, LowValueAsDouble, HighValueAsDouble, lowValueRange, highValueRange, forward),
                        _ => searcher.BetweenAggregation(metadata, LowValue, HighValue, lowValueRange, highValueRange, forward)
                    };
                }
                
                if (LowValue != null)
                {
                    return type switch
                    {
                        RangeType.Double or RangeType.Long => searcher.GreaterAggregationBuilder(metadata, LowValueAsDouble, lowValueRange, forward),
                        _ => searcher.GreaterAggregationBuilder(metadata, LowValue, lowValueRange, forward)
                    };
                }

                return type switch
                {
                    RangeType.Double or RangeType.Long => searcher.LowAggregationBuilder(metadata, HighValueAsDouble, highValueRange, forward),
                    _ => searcher.LowAggregationBuilder(metadata, HighValue, highValueRange, forward)
                };
            }

            private UnaryMatchOperation RangeTypeToCoraxRange(Operation o) => o switch
            {
                Operation.LowerThan => UnaryMatchOperation.LessThan,
                Operation.GreaterThan => UnaryMatchOperation.GreaterThan,
                Operation.Equal => UnaryMatchOperation.Equals,
                Operation.LowerOrEqualThan => UnaryMatchOperation.LessThanOrEqual,
                Operation.GreaterOrEqualThan => UnaryMatchOperation.GreaterThanOrEqual,
                Operation.None => UnaryMatchOperation.None,
                _ => throw new ArgumentOutOfRangeException(nameof(o), o, null)
            };
            
            public bool IsMatch(double value)
            {              
                var leftSide = _leftSide switch
                {
                    Operation.None => true,
                    Operation.GreaterThan => value > LowValueAsDouble,
                    Operation.GreaterOrEqualThan => value >= LowValueAsDouble,
                    _ => ThrowOnUnsupportedType(_leftSide)
                };
                
                var rightSide = _rightSide switch
                {
                    Operation.None => true,
                    Operation.LowerThan => value < HighValueAsDouble,
                    Operation.LowerOrEqualThan => value <= HighValueAsDouble,
                    _ => ThrowOnUnsupportedType(_rightSide)
                };

                return leftSide & rightSide;
            }
            
            public bool IsMatch(long value)
            {
                var leftSide = _leftSide switch
                {
                    Operation.None => true,
                    Operation.GreaterThan => value > LowValueAsLong,
                    Operation.GreaterOrEqualThan => value >= LowValueAsLong,
                    _ => ThrowOnUnsupportedType(_leftSide)

                };
                
                var rightSide = _rightSide switch
                {
                    Operation.None => true,
                    Operation.LowerThan => value < HighValueAsLong,
                    Operation.LowerOrEqualThan => value <= HighValueAsLong,
                    _ => ThrowOnUnsupportedType(_rightSide)
                };

                return leftSide & rightSide;
            }
            
            public bool IsMatch(ReadOnlySpan<byte> value)
            {
                var compareLow = LowValue == null
                        ? -1
                        : value.SequenceCompareTo(LowValueAsBytes);
                var compareHigh = HighValue == null 
                    ? 1 
                    : value.SequenceCompareTo(HighValueAsBytes);

                return CalculateResult(in compareLow, in compareHigh);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool CalculateResult(in int compareLow, in int compareHigh)
            {
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

            [DoesNotReturn]
            private bool ThrowOnUnsupportedType(Operation operation)
            {
                throw new NotSupportedException($"{nameof(CoraxParsedRange)} is not supporting {operation} comparison. This is a bug.");
            }
            
            [Flags]
            private enum Operation
            {
                None = 0, 
                LowerThan = 1,
                GreaterThan = 1 << 1,
                Equal = 1 << 2,
                LowerOrEqualThan = LowerThan | Equal,
                GreaterOrEqualThan = GreaterThan | Equal,
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

        public sealed class FacetResult
        {
            public FacetResult()
            {
                Aggregations = new Dictionary<FacetAggregationField, Aggregation>();
            }

            public string AggregateBy;

            public Dictionary<FacetAggregationField, Aggregation> Aggregations;

            public RangeType RangeType;

            public List<ParsedRange> Ranges;

            public Raven.Client.Documents.Queries.Facets.FacetResult Result;

            public List<string> SortedIds;

            public FacetOptions Options;

            public sealed class Aggregation
            {
                public bool Sum;

                public bool Min;

                public bool Max;

                public bool Average;
            }
        }

        [DoesNotReturn]
        private static void ThrowDifferentTypesOfRangeValues(FacetQuery query, RangeType type1, RangeType type2, string field)
        {
            throw new InvalidQueryException(
                $"Expected to get values of the same type in range expressions while it got {type1} and {type2} for a field '{field}'",
                query.Query.Metadata.QueryText, query.Query.QueryParameters);
        }

        [DoesNotReturn]
        private static void ThrowUnknownFacetType(FacetBase facet)
        {
            throw new InvalidOperationException($"Unsupported facet type: {facet.GetType().FullName}");
        }

        [DoesNotReturn]
        private static void ThrowWhenOptionsAreUsedForRangeFacet()
        {
            throw new NotSupportedException($"Options are not supported in range facets.");
        }

        [DoesNotReturn]
        private static void ThrowRangeDefinedOnDifferentFields(FacetQuery query, string fieldName, string differentField)
        {
            throw new InvalidQueryException($"Facet ranges must be defined on the same field while we got '{fieldName}' and '{differentField}' used in the same faced",
                query.Query.Metadata.QueryText, query.Query.QueryParameters);
        }

        [DoesNotReturn]
        private static void ThrowInvalidFieldInFacetQuery(FacetQuery query, SelectField field)
        {
            throw new InvalidQueryException(
                $"It encountered a field in SELECT clause which is not a facet. Field: {field.Name}", query.Query.Metadata.QueryText,
                query.Query.QueryParameters);
        }

        [DoesNotReturn]
        private static void ThrowUnsupportedRangeOperator(FacetQuery query, OperatorType op)
        {
            throw new InvalidQueryException($"Unsupported operator in a range of a facet query: {op}", query.Query.Metadata.QueryText, query.Query.QueryParameters);
        }

        [DoesNotReturn]
        private static void ThrowUnsupportedRangeExpression(FacetQuery query, QueryExpression expression)
        {
            throw new InvalidQueryException($"Unsupported range expression of a facet query: {expression.GetType().Name}. Text: {expression.GetText(query.Query)}.", query.Query.Metadata.QueryText, query.Query.QueryParameters);
        }
    }
}

