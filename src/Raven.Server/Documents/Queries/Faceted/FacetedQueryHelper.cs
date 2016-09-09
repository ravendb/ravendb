using System;
using System.Collections.Generic;
using System.Globalization;
using Lucene.Net.Util;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;
using Raven.Client.Extensions;
using Raven.Server.Documents.Indexes;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Server.Documents.Queries.Faceted
{
    public static class FacetedQueryHelper
    {
        private static readonly Dictionary<Type, object> NumericalTypes = new Dictionary<Type, object>
        {
            { typeof(decimal), null },
            { typeof(int), null },
            { typeof(long), null },
            { typeof(short), null },
            { typeof(float), null },
            { typeof(double), null }
        };

        public static bool IsAggregationNumerical(FacetAggregation aggregation)
        {
            switch (aggregation)
            {
                case FacetAggregation.Average:
                case FacetAggregation.Count:
                case FacetAggregation.Max:
                case FacetAggregation.Min:
                case FacetAggregation.Sum:
                    return true;
                default:
                    return false;
            }
        }

        public static bool IsAggregationTypeNumerical(string aggregationType)
        {
            if (aggregationType == null)
                return false;
            var type = Type.GetType(aggregationType, false, true);
            if (type == null)
                return false;

            return NumericalTypes.ContainsKey(type);
        }

        public static string GetRangeName(string field, string text, Dictionary<string, IndexField> fields)
        {
            var sortOptions = GetSortOptionsForFacet(field, fields);
            switch (sortOptions)
            {
                case SortOptions.None:
                case SortOptions.String:
                case SortOptions.StringVal:
                    //case SortOptions.Custom: // TODO [arek]
                    return text;
                case SortOptions.NumericLong:
                    if (IsStringNumber(text))
                        return text;
                    return NumericUtils.PrefixCodedToLong(text).ToInvariantString();
                case SortOptions.NumericDouble:
                    if (IsStringNumber(text))
                        return text;
                    return NumericUtils.PrefixCodedToDouble(text).ToInvariantString();
                default:
                    throw new ArgumentException($"Can't get range name from '{sortOptions}' sort option");
            }
        }

        public static SortOptions GetSortOptionsForFacet(string field, Dictionary<string, IndexField> fields)
        {
            if (field.EndsWith(Constants.Indexing.Fields.RangeFieldSuffix))
                field = field.Substring(0, field.Length - Constants.Indexing.Fields.RangeFieldSuffix.Length);

            IndexField value;
            if (fields.TryGetValue(field, out value) == false || value.SortOption.HasValue == false)
                return SortOptions.None;

            return value.SortOption.Value;
        }

        public static string TryTrimRangeSuffix(string fieldName)
        {
            return fieldName.EndsWith(Constants.Indexing.Fields.RangeFieldSuffix) ? fieldName.Substring(0, fieldName.Length - Constants.Indexing.Fields.RangeFieldSuffix.Length) : fieldName;
        }

        public static bool IsStringNumber(string value)
        {
            return string.IsNullOrEmpty(value) == false && char.IsDigit(value[0]);
        }
    }
}