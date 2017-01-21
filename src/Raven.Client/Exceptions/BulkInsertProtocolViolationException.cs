using System;

namespace Raven.Client
{

    public class BulkInsertProtocolViolationException : Exception
    {
        public BulkInsertProtocolViolationException(string message) : base(message)
        {
        }
    }

    public class BulkInsertAbortedException : Exception
    {
        public BulkInsertAbortedException(string message) : base(message)
        {
        }
    }
}
