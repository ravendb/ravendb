using System;

namespace Raven.Server.Exceptions
{
    public class IndexInvalidException : Exception
    {
        public IndexInvalidException()
        {
        }

        public IndexInvalidException(Exception e)
            : base(e.Message, e)
        {
        }

        public IndexInvalidException(string message)
            : base(message)
        {
        }

        public IndexInvalidException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}