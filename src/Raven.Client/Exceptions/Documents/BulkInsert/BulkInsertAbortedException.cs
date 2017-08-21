using System;

namespace Raven.Client.Exceptions.Documents.BulkInsert
{
    public class BulkInsertAbortedException : RavenException
    {
        public BulkInsertAbortedException(string message) : base(message)
        {
        }

        public BulkInsertAbortedException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}