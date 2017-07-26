using System;

namespace Raven.Client.Exceptions
{
    public class InvalidQueryException : RavenException
    {
        public InvalidQueryException(string message, string queryText)
            : base($"{message}{Environment.NewLine}Query: {queryText}")
        {

        }
    }
}