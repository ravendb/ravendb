using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Lucene.Net.Util;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Sparrow;

namespace Raven.Server.Documents.Queries.Faceted
{
    public static class FacetedQueryParser
    {
        public static Dictionary<string, FacetResult> Parse(QueryMetadata metadata, out Dictionary<string, Facet> defaultFacets, out Dictionary<string, List<ParsedRange>> rangeFacets)
        {
            var results = new Dictionary<string, FacetResult>();
            defaultFacets = new Dictionary<string, Facet>();
            rangeFacets = new Dictionary<string, List<ParsedRange>>();
            foreach (var field in metadata.SelectFields)
            {
                if (field.IsFacet == false)
                    throw new InvalidOperationException("Should not happen!");

                var facetField = (FacetField)field;

                var facet = new Facet
                {
                    Name = facetField.Name,
                    DisplayName = facetField.Alias,
                    Aggregations = facetField.Aggregations,
                    Options = FacetOptions.Default // TODO [ppekrol]
                };

                var key = string.IsNullOrWhiteSpace(facet.DisplayName) ? facet.Name : facet.DisplayName;

                defaultFacets[key] = facet;

                if (facet.Ranges?.Count == 0)
                {
                    results[key] = new FacetResult
                    {
                        Name = key
                    };
                }
                else
                {
                    rangeFacets[key] = facet.Ranges
                        .Select(range => ParseRange(facet.Name, range))
                        .ToList();

                    results[key] = new FacetResult
                    {
                        Name = key,
                        Values = facet.Ranges
                            .Select(range => new FacetValue
                            {
                                Range = range
                            })
                            .ToList()
                    };
                }
            }

            return results;
        }

        private static ParsedRange ParseRange(string field, string range)
        {
            var parts = range.Split(new[] { " TO " }, 2, StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length != 2)
                throw new ArgumentException("Could not understand range query: " + range);

            var trimmedLow = parts[0].Trim();
            var trimmedHigh = parts[1].Trim();
            var parsedRange = new ParsedRange
            {
                Field = field,
                RangeText = range,
                LowInclusive = IsInclusive(trimmedLow[0]),
                HighInclusive = IsInclusive(trimmedHigh[trimmedHigh.Length - 1]),
                LowValue = trimmedLow.Substring(1),
                HighValue = trimmedHigh.Substring(0, trimmedHigh.Length - 1)
            };

            parsedRange.LowValue = ConvertFieldValue(field, parsedRange.LowValue);
            parsedRange.HighValue = ConvertFieldValue(field, parsedRange.HighValue);

            parsedRange.LowValue = UnescapeValueIfNecessary(parsedRange.LowValue);
            parsedRange.HighValue = UnescapeValueIfNecessary(parsedRange.HighValue);

            return parsedRange;
        }

        private static string UnescapeValueIfNecessary(string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;

            var unescapedValue = QueryBuilder.Unescape(value);

            if (DateTime.TryParseExact(unescapedValue, DefaultFormat.OnlyDateTimeFormat, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out DateTime _))
                return unescapedValue;

            return value;
        }

        private static string ConvertFieldValue(string field, string value)
        {
            if (NumberUtil.IsNull(value))
                return null;

            var rangeType = FieldUtil.GetRangeTypeFromFieldName(field);
            switch (rangeType)
            {
                case RangeType.Long:
                    var longValue = NumberUtil.StringToLong(value);
                    return NumericUtils.LongToPrefixCoded(longValue.Value);
                case RangeType.Double:
                    var doubleValue = NumberUtil.StringToDouble(value);
                    return NumericUtils.DoubleToPrefixCoded(doubleValue.Value);
                default:
                    return value;
            }
        }

        private static bool IsInclusive(char ch)
        {
            switch (ch)
            {
                case '[':
                case ']':
                    return true;
                case '{':
                case '}':
                    return false;
                default:
                    throw new ArgumentException("Could not understand range prefix: " + ch);
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
    }
}
