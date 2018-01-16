using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Server.Exceptions
{
    public class SerializationNestedLevelTooDeepException : Exception
    {
        public SerializationNestedLevelTooDeepException(string message) : base(message)
        {
        }
    }
}
