using System;

namespace Raven.Client.Exceptions.Database
{
    public sealed class DatabaseSchemaErrorException : RavenException
    {
        public DatabaseSchemaErrorException()
        {
        }

        public DatabaseSchemaErrorException(string message)
            : base(message)
        {
        }
        public DatabaseSchemaErrorException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}
