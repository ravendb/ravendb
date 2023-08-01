using System;
using System.Text;
using Sparrow.Json;

namespace Raven.Client.Exceptions
{
    public sealed class InvalidQueryException : RavenException
    {
        private InvalidQueryException()
        {
        }

        public InvalidQueryException(string message)
            : base(message)
        {
        }

        public InvalidQueryException(string message, Exception inner)
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
                string parametersString = null;
                try
                {
                    parametersString = parameters.ToString();
                }
                catch (InsufficientExecutionStackException)
                {
                    parametersString = "Parameters are too big to attach them in exception.";
                }

                if (parametersString != null)
                    result.Append(Environment.NewLine)
                        .Append("Parameters: ")
                        .Append(parametersString);
            }
            
            return result.ToString();
        }
    }
}
