using System;
using Raven.Client.Exceptions;

namespace Raven.Client.Documents.Exceptions.Indexes
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