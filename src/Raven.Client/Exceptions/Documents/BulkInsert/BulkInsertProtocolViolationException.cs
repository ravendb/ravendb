namespace Raven.Client.Exceptions.Documents.BulkInsert
{
    public class BulkInsertProtocolViolationException : RavenException
    {
        public BulkInsertProtocolViolationException(string message) : base(message)
        {
        }
    }
}
