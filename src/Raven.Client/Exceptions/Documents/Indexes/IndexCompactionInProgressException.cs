using System;

namespace Raven.Client.Exceptions.Documents.Indexes
{
    internal sealed class IndexCompactionInProgressException : RavenException
    {
        public IndexCompactionInProgressException()
        {
        }

        public IndexCompactionInProgressException(string message)
            : base(message)
        {
        }

        public IndexCompactionInProgressException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
