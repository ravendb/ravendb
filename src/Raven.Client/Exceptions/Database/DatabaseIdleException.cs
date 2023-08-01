using System;

namespace Raven.Client.Exceptions.Database
{
    public sealed class DatabaseIdleException : RavenException
    {
        public DatabaseIdleException()
        {
        }

        public DatabaseIdleException(string message)
            : base(message)
        {
        }

        public DatabaseIdleException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
