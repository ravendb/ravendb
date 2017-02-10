namespace Raven.Client.Exceptions.BulkInsert
{
    public class BulkInsertProtocolViolationException : RavenException
    {
        public BulkInsertProtocolViolationException(string message) : base(message)
        {
        }
    }
}
