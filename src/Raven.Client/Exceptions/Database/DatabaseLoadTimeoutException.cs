namespace Raven.Client.Exceptions.Database
{
    public class DatabaseLoadTimeoutException : RavenException
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