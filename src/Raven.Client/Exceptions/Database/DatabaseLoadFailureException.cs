using System;

namespace Raven.Client.Exceptions.Database
{
    public sealed class DatabaseLoadFailureException : RavenException
    {
        public DatabaseLoadFailureException()
        {
        }

        public DatabaseLoadFailureException(string message)
            : base(message)
        {
        }
        public DatabaseLoadFailureException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}