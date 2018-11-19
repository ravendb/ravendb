using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Client.Exceptions.Database
{
    public class DatabaseSchemaErrorException : RavenException
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
