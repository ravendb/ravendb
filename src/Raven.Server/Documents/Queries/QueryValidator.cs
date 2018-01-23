using System.Collections.Generic;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public static class QueryValidator
    {
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
            if (arguments.Count != 1)
                throw new InvalidQueryException("Method 'wkt()' expects one argument to be provided", queryText, parameters);

            var valueToken = arguments[0] as ValueExpression;

            if (valueToken == null)
                throw new InvalidQueryException($"Method 'wkt()' expects value token as an argument, got {arguments[0]} type", queryText, parameters);
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
    }
}
