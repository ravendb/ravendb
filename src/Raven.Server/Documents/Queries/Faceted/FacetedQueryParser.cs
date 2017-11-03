using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Queries.Faceted
{
    public static class FacetedQueryParser
    {
        public static Dictionary<string, FacetResult> Parse(DocumentsOperationContext context, QueryMetadata metadata, out Dictionary<string, Facet> defaultFacets, out Dictionary<string, List<ParsedRange>> rangeFacets)
        {
            DocumentsTransaction tx = null;

            try
            {
                var results = new Dictionary<string, FacetResult>();
                defaultFacets = new Dictionary<string, Facet>();
                rangeFacets = new Dictionary<string, List<ParsedRange>>();
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

                        foreach (var f in document.Facets)
                        {
                            if (f.Options == null)
                                f.Options = FacetOptions.Default;

                            Process(f, null, defaultFacets, rangeFacets, results);
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

                    Process(facet, facetField.Ranges, defaultFacets, rangeFacets, results);
                }

                return results;
            }
            finally
            {
                tx?.Dispose();
            }
        }

        private static void Process(Facet facet, List<QueryExpression> facetRanges, Dictionary<string, Facet> defaultFacets, Dictionary<string, List<ParsedRange>> rangeFacets, Dictionary<string, FacetResult> results)
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

                var ranges = rangeFacets[key] = new List<ParsedRange>();
                foreach (var range in facetRanges)
                {
                    var parsedRange = ParseRange(facet.Name, range);
                    
                    ranges.Add(parsedRange);
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
                return new ParsedRange
                {
                    Field = field,
                    HighInclusive = true,
                    HighValue = bee.Max.Token,
                    LowInclusive = true,
                    LowValue = bee.Min.Token,
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
                        if (string.Equals(field, fieldName, StringComparison.OrdinalIgnoreCase) == false)
                            throw new InvalidOperationException("TODO ppekrol");

                        var fieldValue = ExtractFieldValue(be);

                        var range = new ParsedRange
                        {
                            Field = field,
                            RangeText = expression.GetText()
                        };

                        if (be.Operator == OperatorType.LessThan || be.Operator == OperatorType.LessThanEqual)
                        {
                            range.HighValue = fieldValue;
                            range.HighInclusive = be.Operator == OperatorType.LessThanEqual;

                            return range;
                        }

                        if (be.Operator == OperatorType.GreaterThan || be.Operator == OperatorType.GreaterThanEqual)
                        {
                            range.LowValue = fieldValue;
                            range.LowInclusive = be.Operator == OperatorType.GreaterThanEqual;

                            return range;
                        }

                        return range;
                    case OperatorType.And:
                        var left = ParseRange(field, be.Left);
                        var right = ParseRange(field, be.Right);

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

        private static string ExtractFieldValue(BinaryExpression be)
        {
            if (be.Right is ValueExpression rve)
                return rve.Token;

            throw new InvalidOperationException("TODO ppekrol");
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
    }
}
