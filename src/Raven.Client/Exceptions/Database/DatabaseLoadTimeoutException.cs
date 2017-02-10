namespace Raven.NewClient.Client.Exceptions.Database
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