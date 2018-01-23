using System;

namespace Raven.Server.Exceptions
{
    public class SerializationNestedLevelTooDeepException : Exception
    {
        public SerializationNestedLevelTooDeepException(string message) : base(message)
        {
        }
    }
}
