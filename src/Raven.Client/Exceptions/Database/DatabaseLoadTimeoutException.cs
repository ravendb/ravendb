namespace Raven.Client.Exceptions.Database
{
    public sealed class DatabaseLoadTimeoutException : RavenException
    {
        public DatabaseLoadTimeoutException()
        {
        }

        public DatabaseLoadTimeoutException(string message)
            : base(message)
        {
        }
    }
}