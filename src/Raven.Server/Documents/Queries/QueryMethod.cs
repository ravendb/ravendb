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

            if (string.Equals(methodName, "boost", StringComparison.OrdinalIgnoreCase))
                return MethodType.Boost;

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

            if (string.Equals(methodName, "within", StringComparison.OrdinalIgnoreCase))
                return MethodType.Within;

            if (string.Equals(methodName, "contains", StringComparison.OrdinalIgnoreCase))
                return MethodType.Contains;

            if (string.Equals(methodName, "disjoint", StringComparison.OrdinalIgnoreCase))
                return MethodType.Disjoint;

            if (string.Equals(methodName, "intersects", StringComparison.OrdinalIgnoreCase))
                return MethodType.Intersects;

            if (string.Equals(methodName, "circle", StringComparison.OrdinalIgnoreCase))
                return MethodType.Circle;

            if (string.Equals(methodName, "wkt", StringComparison.OrdinalIgnoreCase))
                return MethodType.Wkt;

            if (string.Equals(methodName, "point", StringComparison.OrdinalIgnoreCase))
                return MethodType.Point;

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
