using System;
using System.Collections.Generic;
using Raven.Abstractions.Indexing;
using Raven.Client.Data;

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
    }
}