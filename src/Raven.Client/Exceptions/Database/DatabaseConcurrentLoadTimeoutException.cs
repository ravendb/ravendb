using System;

namespace Raven.Client.Exceptions.Database
{
    public class DatabaseConcurrentLoadTimeoutException : RavenException
    {
        public string Caller { get; set; }

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
