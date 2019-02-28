using System;

namespace Raven.Client.Exceptions.Database
{
    public class DatabaseRestoringException : RavenException
    {
        public DatabaseRestoringException()
        {
        }

        public DatabaseRestoringException(string message)
            : base(message)
        {
        }

        public DatabaseRestoringException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
