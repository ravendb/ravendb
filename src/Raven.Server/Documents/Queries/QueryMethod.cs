using System;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
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

            if (string.Equals(methodName, "regex", StringComparison.OrdinalIgnoreCase))
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

            if (string.Equals(methodName, "timeseries", StringComparison.OrdinalIgnoreCase))
                return MethodType.TimeSeries;

            if (string.Equals(methodName, "last", StringComparison.OrdinalIgnoreCase))
                return MethodType.Last;

            if (throwIfNoMatch == false)
                return MethodType.Unknown;

            throw new NotSupportedException($"Method '{methodName}' is not supported.");
        }

        public static void ThrowMethodNotSupported(MethodType methodType, string queryText, BlittableJsonReaderObject parameters)
        {
            throw new InvalidQueryException($"Method '{methodType}' is not supported.", queryText, parameters);
        }

        public static class TimeSeries
        {
            public static TimeValue LastWithTime(MethodExpression expression, string queryText, BlittableJsonReaderObject parameters)
            {
                try
                {
                    if (expression.Arguments.Count != 2)
                        throw new InvalidOperationException("Method 'last()' expects two arguments to be provided");

                    var duration = Helpers.GetLong(expression.Arguments[0], parameters);
                    var timeValueUnitAsString = Helpers.GetString(expression.Arguments[1], parameters);

                    var offset = 0;
                    return RangeGroup.ParseTimePeriodFromString(duration, timeValueUnitAsString, ref offset);
                }
                catch (Exception e)
                {
                    throw new InvalidQueryException("Could not parse last() method.", queryText, parameters, e);
                }
            }

            public static int LastWithCount(MethodExpression expression, string queryText, BlittableJsonReaderObject parameters)
            {
                try
                {
                    if (expression.Arguments.Count != 1)
                        throw new InvalidOperationException("Method 'last()' with count expects one arguments to be provided");

                    var duration = (int)Helpers.GetLong(expression.Arguments[0], parameters);
                    return duration;
                }
                catch (Exception e)
                {
                    throw new InvalidQueryException("Could not parse last() method.", queryText, parameters, e);
                }
            }
        }

        public static class Helpers
        {
            public static string GetString(QueryExpression expression, BlittableJsonReaderObject parameters)
            {
                switch (expression)
                {
                    case ValueExpression ve:
                        var value = ve.GetValue(parameters);
                        switch (value)
                        {
                            case string s:
                                return s;
                            case StringSegment ss:
                                return ss.ToString();
                            default:
                                throw new InvalidOperationException($"Expected to get a value with type of string, but got {value.GetType()}.");
                        }
                    default:
                        throw new InvalidOperationException($"Expected to get a {nameof(ValueExpression)} but got {expression.GetType()}.");
                }
            }

            public static long GetLong(QueryExpression expression, BlittableJsonReaderObject parameters)
            {
                switch (expression)
                {
                    case ValueExpression ve:
                        var value = ve.GetValue(parameters);
                        switch (value)
                        {
                            case long l:
                                return l;
                            case string s:
                                return Parse(s);
                            case StringSegment ss:
                                return Parse(ss.ToString());
                            default:
                                throw new InvalidOperationException($"Expected to get a value with type of long or string, but got {value.GetType()}.");
                        }
                    default:
                        throw new InvalidOperationException($"Expected to get a {nameof(ValueExpression)} but got {expression.GetType()}.");
                }

                static long Parse(string s)
                {
                    if (long.TryParse(s, out var sAsLong) == false)
                        throw new InvalidOperationException($"Could not parse the provided string '{s}' to long.");

                    return sAsLong;
                }
            }
        }
    }
}
