using System;

namespace Raven.Server.Exceptions
{
    public sealed class IndexWriteException : Exception
    {
        public IndexWriteException()
        {
        }

        public IndexWriteException(Exception e) : base (e.Message, e)
        {
        }

        public IndexWriteException(string message)
            : base(message)
        {
        }

        public IndexWriteException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}