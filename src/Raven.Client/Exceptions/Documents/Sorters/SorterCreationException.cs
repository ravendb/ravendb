using System;

namespace Raven.Client.Exceptions.Documents.Sorters
{
    public class SorterCreationException : RavenException
    {
        public SorterCreationException()
        {
        }

        public SorterCreationException(string message) : base(message)
        {
        }

        public SorterCreationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
