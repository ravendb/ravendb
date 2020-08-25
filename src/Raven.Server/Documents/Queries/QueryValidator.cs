using System.Collections.Generic;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public static class QueryValidator
    {
        public static void ValidateTimings(List<QueryExpression> arguments, string queryText, BlittableJsonReaderObject parameters)
        {
            if (arguments.Count != 0)
                throw new InvalidQueryException("Method 'timings()' expects zero arguments to be provided", queryText, parameters);
        }

        public static void ValidateExplanations(List<QueryExpression> arguments, string queryText, BlittableJsonReaderObject parameters)
        {
            if (arguments.Count > 1)
                throw new InvalidQueryException("Method 'explanations()' expects zero or one argument(s) to be provided", queryText, parameters);

            for (var i = 0; i < arguments.Count; i++)
            {
                switch (arguments[i])
                {
                    case ValueExpression _:
                        break;
                    default:
                        throw new InvalidQueryException($"Method 'explanations()' expects value token as an argument at index {i}, got {arguments[0]} type", queryText, parameters);
                }
            }
        }

        public static void ValidateHighlight(List<QueryExpression> arguments, string queryText, BlittableJsonReaderObject parameters)
        {
            if (arguments.Count < 3 || arguments.Count > 4)
                throw new InvalidQueryException("Method 'highlight()' expects three or four arguments to be provided", queryText, parameters);

            for (var i = 0; i < arguments.Count; i++)
            {
                switch (arguments[i])
                {
                    case FieldExpression _:
                    case ValueExpression _:
                        break;
                    default:
                        throw new InvalidQueryException($"Method 'highlight()' expects field or value token as an argument at index {i}, got {arguments[0]} type", queryText, parameters);
                }
            }
        }

        public static void ValidateCircle(List<QueryExpression> arguments, string queryText, BlittableJsonReaderObject parameters)
        {
            if (arguments.Count < 3 || arguments.Count > 4)
                throw new InvalidQueryException("Method 'circle()' expects three or four arguments to be provided", queryText, parameters);

            for (var i = 0; i < arguments.Count; i++)
            {
                var argument = arguments[i] as ValueExpression;
                if (argument == null)
                    throw new InvalidQueryException($"Method 'circle()' expects value token as an argument at index {i}, got {arguments[i]} type", queryText, parameters);
            }
        }

        public static void ValidateWkt(List<QueryExpression> arguments, string queryText, BlittableJsonReaderObject parameters)
        {
            if (arguments.Count == 0 || arguments.Count > 2)
                throw new InvalidQueryException("Method 'wkt()' expects one or two argument to be provided", queryText, parameters);

            var valueToken = arguments[0] as ValueExpression;

            if (valueToken == null)
                throw new InvalidQueryException($"Method 'wkt()' expects value token as an argument, got {arguments[0]} type", queryText, parameters);

            if (arguments.Count == 2)
            {
                valueToken = arguments[1] as ValueExpression;

                if (valueToken == null)
                    throw new InvalidQueryException($"Method 'wkt()' expects value token as an argument, got {arguments[1]} type", queryText, parameters);
            }
        }

        public static void ValidatePoint(List<QueryExpression> arguments, string queryText, BlittableJsonReaderObject parameters)
        {
            if (arguments.Count != 2)
                throw new InvalidQueryException("Method 'point()' expects two arguments to be provided", queryText, parameters);

            for (var i = 0; i < arguments.Count; i++)
            {
                var argument = arguments[i] as ValueExpression;
                if (argument == null)
                    throw new InvalidQueryException($"Method 'point()' expects value token as an argument at index {i}, got {arguments[i]} type", queryText, parameters);
            }
        }

        public static void ValidateIncludeCounter(List<QueryExpression> arguments, string queryText, BlittableJsonReaderObject parameters)
        {
            if (arguments.Count == 0)
                return;

            switch (arguments[0])
            {
                case ValueExpression _:
                case FieldExpression _:
                    break;
                default:
                    throw new InvalidQueryException("Method 'counters()' expects value token (counter name) or field token (source alias) " +
                                                    $"as an argument at index 0, got {arguments[0]} type", queryText, parameters);
            }

            for (var i = 1; i < arguments.Count; i++)
            {
                switch (arguments[i])
                {
                    case ValueExpression _:
                        break;
                    default:
                        throw new InvalidQueryException($"Method 'counters()' expects value token as an argument at index {i}, got {arguments[0]} type", queryText, parameters);
                }
            }
        }

        public static void ValidateIncludeTimeseries(List<QueryExpression> arguments, string queryText, BlittableJsonReaderObject parameters)
        {
            if (arguments.Count == 0)
                return;

            switch (arguments[0])
            {
                case ValueExpression _:
                case FieldExpression _:
                case MethodExpression _:
                    break;
                default:
                    throw new InvalidQueryException("Method 'timeseries()' expects value token (timeseries name) or field token (source alias) " +
                                                    $"as an argument at index 0, got {arguments[0]} type", queryText, parameters);
            }

            for (var i = 1; i < arguments.Count; i++)
            {
                switch (arguments[i])
                {
                    case ValueExpression _:
                    case MethodExpression _:
                        break;
                    default:
                        throw new InvalidQueryException($"Method 'timeseries()' expects value token as an argument or method at index {i}, got {arguments[0]} type", queryText, parameters);
                }
            }
        }

        public static void ValidateIncludeCompareExchangeValue(List<QueryExpression> arguments, string queryText, BlittableJsonReaderObject parameters)
        {
            if (arguments.Count != 1)
                throw new InvalidQueryException("Method 'cmpxchg()' expects one argument to be provided", queryText, parameters);

            switch (arguments[0])
            {
                case ValueExpression _:
                case FieldExpression _:
                    break;
                default:
                    throw new InvalidQueryException($"Method 'cmpxchg()' expects field or value token as an argument at index {0}, got {arguments[0]} type", queryText, parameters);
            }
        }
    }
}
