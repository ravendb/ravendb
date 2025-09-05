using System;

namespace Raven.Client.Exceptions.Database
{
    public sealed class DatabaseDisabledException : RavenException
    {
        public DatabaseDisabledException()
        {
        }

        public DatabaseDisabledException(string message)
            : base(message)
        {
        }

        public DatabaseDisabledException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}