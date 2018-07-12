using System;

namespace Raven.Client.Exceptions.Documents.BulkInsert
{
    public class BulkInsertProtocolViolationException : RavenException
    {
        public BulkInsertProtocolViolationException(string message) : base(message)
        {
        }

        public BulkInsertProtocolViolationException(string message, Exception inner) : base(message, inner)
        {

        }
    }
}
