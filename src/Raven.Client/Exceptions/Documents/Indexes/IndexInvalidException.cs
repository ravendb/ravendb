using System;

namespace Raven.Client.Exceptions.Documents.Indexes
{
    public class IndexInvalidException : RavenException
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