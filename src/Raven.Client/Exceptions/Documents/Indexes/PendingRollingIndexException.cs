namespace Raven.Client.Exceptions.Documents.Indexes
{
    public sealed class PendingRollingIndexException : RavenException
    {
        public PendingRollingIndexException()
        {

        }

        public PendingRollingIndexException(string message) : base(message)
        {

        }
    }
}
