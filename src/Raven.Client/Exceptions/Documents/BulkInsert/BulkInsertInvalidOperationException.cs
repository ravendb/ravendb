using System;

namespace Raven.Client.Exceptions.Documents.BulkInsert
{
    public class BulkInsertInvalidOperationException : BulkInsertClientException
    {
        public BulkInsertInvalidOperationException(string message) : base(message)
        {
        }

        public BulkInsertInvalidOperationException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
