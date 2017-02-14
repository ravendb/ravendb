using Raven.Client.Exceptions;

namespace Raven.Client.Documents.Exceptions.BulkInsert
{
    public class BulkInsertAbortedException : RavenException
    {
        public BulkInsertAbortedException(string message) : base(message)
        {
        }
    }
}