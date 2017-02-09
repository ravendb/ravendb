using System;

namespace Raven.NewClient.Client.Exceptions.Database
{
    public class DatabaseConcurrentLoadTimeoutException : RavenException
    {
        public DatabaseConcurrentLoadTimeoutException(string message)
            : base(message)
        {
        }

        public DatabaseConcurrentLoadTimeoutException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}