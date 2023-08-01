using System;

namespace Raven.Server.Exceptions
{
    public sealed class SerializationNestedLevelTooDeepException : Exception
    {
        public SerializationNestedLevelTooDeepException(string message) : base(message)
        {
        }
    }
}
