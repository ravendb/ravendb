using System;

namespace Raven.Client.Exceptions.Documents.Indexes
{
    public class IndexCreationException: RavenException
    {
        public IndexCreationException()
        {            
        }

        public IndexCreationException(string message) : base(message)
        {
        }

        public IndexCreationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
