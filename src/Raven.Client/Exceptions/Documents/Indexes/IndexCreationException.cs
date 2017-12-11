using System;
using Raven.Client.ServerWide;

namespace Raven.Client.Exceptions.Documents.Indexes
{
    public class IndexCreationException: Exception
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
