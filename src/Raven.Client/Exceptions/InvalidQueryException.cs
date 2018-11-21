using System;
using System.Text;
using Sparrow.Json;

namespace Raven.Client.Exceptions
{
    public class InvalidQueryException : RavenException
    {
        private InvalidQueryException()
        {
        }

        public InvalidQueryException(string message)
            : base(message)
        {
        }

        private InvalidQueryException(string message, Exception inner)
            : base(message, inner)
        {
        }

        public InvalidQueryException(string message, string queryText, BlittableJsonReaderObject parameters, Exception e)
            : base(BuildMessage(message, queryText, parameters), e)
        {
        }

        public InvalidQueryException(string message, string queryText, BlittableJsonReaderObject parameters = null)
            : base(BuildMessage(message, queryText, parameters))
        {

        }

        private static string BuildMessage(string message, string queryText, BlittableJsonReaderObject parameters)
        {
            var result = new StringBuilder(message?.Length ?? 0 + queryText?.Length ?? 0);

            result.Append(message)
                .Append(Environment.NewLine)
                .Append("Query: ")
                .Append(queryText);

            if (parameters != null)
            {
                result.Append(Environment.NewLine)
                    .Append("Parameters: ")
                    .Append(parameters);
            }

            return result.ToString();
        }
    }
}
