using System;
using System.Collections.Generic;
using System.Globalization;
using Lucene.Net.Util;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Parser;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries.Faceted
{
    public static class FacetedQueryParser
    {
        public static Dictionary<string, FacetResult> Parse(DocumentsOperationContext context, QueryMetadata metadata, out Dictionary<string, Facet> defaultFacets, out Dictionary<string, (RangeType Type, List<ParsedRange> Ranges)> rangeFacets)
        {
            DocumentsTransaction tx = null;

            try
            {
                var results = new Dictionary<string, FacetResult>();
                defaultFacets = new Dictionary<string, Facet>();
                rangeFacets = new Dictionary<string, (RangeType Type, List<ParsedRange> Ranges)>();
                foreach (var field in metadata.SelectFields)
                {
                    if (field.IsFacet == false)
                        throw new InvalidOperationException("Should not happen!");

                    var facetField = (FacetField)field;
                    if (facetField.FacetSetupDocumentId != null)
                    {
                        if (tx == null)
                            tx = context.OpenReadTransaction();

                        var documentJson = context.DocumentDatabase.DocumentsStorage.Get(context, facetField.FacetSetupDocumentId);
                        if (documentJson == null)
                            throw new InvalidOperationException("TODO ppekrol");

                        var document = (FacetSetup)EntityToBlittable.ConvertToEntity(typeof(FacetSetup), facetField.FacetSetupDocumentId, documentJson.Data, DocumentConventions.Default);

                        QueryParser queryParser = null;
                        List<QueryExpression> facetRanges = null;
                        foreach (var f in document.Facets)
                        {
                            if (f.Options == null)
                                f.Options = FacetOptions.Default;

                            if (f.Ranges != null && f.Ranges.Count > 0)
                            {
                                if (queryParser == null)
                                    queryParser = new QueryParser();

                                if (facetRanges == null)
                                    facetRanges = new List<QueryExpression>();
                                else
                                    facetRanges.Clear();

                                foreach (var range in f.Ranges)
                                {
                                    queryParser.Init(range);

                                    if (queryParser.Expression(out var qr) == false)
                                        throw new InvalidOperationException("TODO ppekrol");

                                    facetRanges.Add(qr);
                                }
                            }

                            ProcessFacet(f, facetRanges, defaultFacets, rangeFacets, results);
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

                    ProcessFacet(facet, facetField.Ranges, defaultFacets, rangeFacets, results);
                }

                return results;
            }
            finally
            {
                tx?.Dispose();
            }
        }

        private static void ProcessFacet(Facet facet, List<QueryExpression> facetRanges, Dictionary<string, Facet> defaultFacets, Dictionary<string, (RangeType Type, List<ParsedRange> Ranges)> rangeFacets, Dictionary<string, FacetResult> results)
        {
            var key = string.IsNullOrWhiteSpace(facet.DisplayName) ? facet.Name : facet.DisplayName;

            defaultFacets[key] = facet;

            if (facetRanges == null || facetRanges.Count == 0)
            {
                results[key] = new FacetResult
                {
                    Name = key
                };
            }
            else
            {
                var result = results[key] = new FacetResult
                {
                    Name = key
                };

                var ranges = rangeFacets[key] = (RangeType.None, new List<ParsedRange>());
                foreach (var range in facetRanges)
                {
                    var parsedRange = ParseRange(facet.Name, range);

                    ranges.Type = parsedRange.RangeType;
                    ranges.Ranges.Add(parsedRange);

                    result.Values.Add(new FacetValue
                    {
                        Range = parsedRange.RangeText
                    });
                }
            }
        }

        private static ParsedRange ParseRange(string field, QueryExpression expression)
        {
            if (expression is BetweenExpression bee)
            {
                var hValue = ConvertFieldValue(bee.Max.Token, bee.Max.Value);
                var lValue = ConvertFieldValue(bee.Min.Token, bee.Min.Value);

                if (hValue.Type != lValue.Type)
                    throw new InvalidOperationException("TODO ppekrol");

                return new ParsedRange
                {
                    Field = field,
                    HighInclusive = true,
                    HighValue = hValue.Value,
                    LowInclusive = true,
                    LowValue = lValue.Value,
                    RangeText = expression.GetText(),
                    RangeType = hValue.Type
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
                        if (string.Equals(field, fieldName, StringComparison.OrdinalIgnoreCase) == false)
                            throw new InvalidOperationException("TODO ppekrol");

                        var r = (ValueExpression)be.Right;
                        var fieldValue = ConvertFieldValue(r.Token, r.Value);

                        var range = new ParsedRange
                        {
                            Field = field,
                            RangeText = expression.GetText(),
                            RangeType = fieldValue.Type
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
                        var left = ParseRange(field, be.Left);
                        var right = ParseRange(field, be.Right);

                        if (left.RangeType != right.RangeType)
                            throw new InvalidOperationException("TODO ppekrol");

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
            public RangeType RangeType;

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
    }
}

