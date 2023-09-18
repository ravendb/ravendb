using System;

namespace Raven.Client.Exceptions.Documents.BulkInsert
{
    public abstract class BulkInsertClientException : RavenException
    {
        protected BulkInsertClientException(string message) : base(message)
        {
        }

        protected BulkInsertClientException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
