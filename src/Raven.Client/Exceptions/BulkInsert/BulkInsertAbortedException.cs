namespace Raven.Client.Exceptions.BulkInsert
{
    public class BulkInsertAbortedException : RavenException
    {
        public BulkInsertAbortedException(string message) : base(message)
        {
        }
    }
}