using System;
using System.Collections.Generic;
using Lucene.Net.Util;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Extensions;

namespace Raven.Server.Documents.Queries.Faceted
{
    public static class FacetedQueryHelper
    {
        private static readonly Dictionary<Type, RangeType> NumericalTypes = new Dictionary<Type, RangeType>
        {
            { typeof(decimal), RangeType.Double },
            { typeof(int), RangeType.Long },
            { typeof(long), RangeType.Long },
            { typeof(short), RangeType.Long },
            { typeof(float), RangeType.Double },
            { typeof(double), RangeType.Double }
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

        public static RangeType GetRangeTypeForAggregationType(string aggregationType)
        {
            if (aggregationType == null)
                return RangeType.None;
            var type = Type.GetType(aggregationType, false, true);
            if (type == null)
                return RangeType.None;

            if (NumericalTypes.TryGetValue(type, out RangeType rangeType) == false)
                return RangeType.None;

            return rangeType;
        }

        public static string GetRangeName(string field, string text)
        {
            var rangeType = FieldUtil.GetRangeTypeFromFieldName(field);
            switch (rangeType)
            {
                case RangeType.Long:
                    return NumericUtils.PrefixCodedToLong(text).ToInvariantString();
                case RangeType.Double:
                    return NumericUtils.PrefixCodedToDouble(text).ToInvariantString();
                default:
                    return text;
            }

        }
    }
}