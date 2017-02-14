using Raven.Client.Exceptions;

namespace Raven.Client.Documents.Exceptions.BulkInsert
{
    public class BulkInsertProtocolViolationException : RavenException
    {
        public BulkInsertProtocolViolationException(string message) : base(message)
        {
        }
    }
}
