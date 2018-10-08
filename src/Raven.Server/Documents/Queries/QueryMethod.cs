using System;
using Raven.Client.Exceptions;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public static class QueryMethod
    {
        public static MethodType GetMethodType(string methodName, bool throwIfNoMatch = true)
        {
            if (string.Equals(methodName, "id", StringComparison.OrdinalIgnoreCase))
                return MethodType.Id;

            if (string.Equals(methodName, "search", StringComparison.OrdinalIgnoreCase))
                return MethodType.Search;

            if (string.Equals(methodName, "cmpxchg", StringComparison.OrdinalIgnoreCase))
                return MethodType.CompareExchange;
            
            if (string.Equals(methodName, "boost", StringComparison.OrdinalIgnoreCase))
                return MethodType.Boost;

            if(string.Equals(methodName,"regex",StringComparison.OrdinalIgnoreCase))
                return MethodType.Regex;

            if (string.Equals(methodName, "startsWith", StringComparison.OrdinalIgnoreCase))
                return MethodType.StartsWith;

            if (string.Equals(methodName, "endsWith", StringComparison.OrdinalIgnoreCase))
                return MethodType.EndsWith;

            if (string.Equals(methodName, "lucene", StringComparison.OrdinalIgnoreCase))
                return MethodType.Lucene;

            if (string.Equals(methodName, "exists", StringComparison.OrdinalIgnoreCase))
                return MethodType.Exists;

            if (string.Equals(methodName, "exact", StringComparison.OrdinalIgnoreCase))
                return MethodType.Exact;

            if (string.Equals(methodName, "intersect", StringComparison.OrdinalIgnoreCase))
                return MethodType.Intersect;

            if (string.Equals(methodName, "count", StringComparison.OrdinalIgnoreCase))
                return MethodType.Count;

            if (string.Equals(methodName, "sum", StringComparison.OrdinalIgnoreCase))
                return MethodType.Sum;

            if (string.Equals(methodName, "spatial.within", StringComparison.OrdinalIgnoreCase))
                return MethodType.Spatial_Within;

            if (string.Equals(methodName, "spatial.contains", StringComparison.OrdinalIgnoreCase))
                return MethodType.Spatial_Contains;

            if (string.Equals(methodName, "spatial.disjoint", StringComparison.OrdinalIgnoreCase))
                return MethodType.Spatial_Disjoint;

            if (string.Equals(methodName, "spatial.intersects", StringComparison.OrdinalIgnoreCase))
                return MethodType.Spatial_Intersects;

            if (string.Equals(methodName, "spatial.circle", StringComparison.OrdinalIgnoreCase))
                return MethodType.Spatial_Circle;

            if (string.Equals(methodName, "spatial.wkt", StringComparison.OrdinalIgnoreCase))
                return MethodType.Spatial_Wkt;

            if (string.Equals(methodName, "spatial.point", StringComparison.OrdinalIgnoreCase))
                return MethodType.Spatial_Point;

            if (string.Equals(methodName, "moreLikeThis", StringComparison.OrdinalIgnoreCase))
                return MethodType.MoreLikeThis;

            if (string.Equals(methodName, "avg", StringComparison.OrdinalIgnoreCase))
                return MethodType.Average;

            if (string.Equals(methodName, "min", StringComparison.OrdinalIgnoreCase))
                return MethodType.Min;

            if (string.Equals(methodName, "max", StringComparison.OrdinalIgnoreCase))
                return MethodType.Max;

            if (string.Equals(methodName, "array", StringComparison.OrdinalIgnoreCase))
                return MethodType.Array;

            if (string.Equals(methodName, "highlight", StringComparison.OrdinalIgnoreCase))
                return MethodType.Highlight;

            if (string.Equals(methodName, "explanations", StringComparison.OrdinalIgnoreCase))
                return MethodType.Explanation;

            if (string.Equals(methodName, "timings", StringComparison.OrdinalIgnoreCase))
                return MethodType.Timings;

            if (string.Equals(methodName, "counters", StringComparison.OrdinalIgnoreCase))
                return MethodType.Counters;

            if (string.Equals(methodName, "fuzzy", StringComparison.OrdinalIgnoreCase))
                return MethodType.Fuzzy;

            if (string.Equals(methodName, "proximity", StringComparison.OrdinalIgnoreCase))
                return MethodType.Proximity;

            if (throwIfNoMatch == false)
                return MethodType.Unknown;

            throw new NotSupportedException($"Method '{methodName}' is not supported.");
        }

        public static void ThrowMethodNotSupported(MethodType methodType, string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Method '{methodType}' is not supported.", queryText, parameters);
        }
    }
}
