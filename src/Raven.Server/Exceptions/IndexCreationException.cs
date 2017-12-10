using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Server.Exceptions
{
    public class IndexCreationException: Exception
    {
        public IndexCreationException()
        {            
        }

        public IndexCreationException(string message) : base(message)
        {
        }

        public IndexCreationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

    }
}
