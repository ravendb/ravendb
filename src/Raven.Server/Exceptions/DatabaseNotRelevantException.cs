using System;

namespace Raven.Server.Exceptions
{
    public class DatabaseNotRelevantException : Exception
    {
        public DatabaseNotRelevantException()
        {
        }

        public DatabaseNotRelevantException(string message) : base(message)
        {
        }

        public DatabaseNotRelevantException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
