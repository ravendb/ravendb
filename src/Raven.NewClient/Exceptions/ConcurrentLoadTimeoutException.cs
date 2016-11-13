using System;
using System.Runtime.Serialization;

namespace Raven.Abstractions.Exceptions
{
    public class ConcurrentLoadTimeoutException : Exception
    {
        public ConcurrentLoadTimeoutException(string message)
            : base(message)
        {
        }

        public ConcurrentLoadTimeoutException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
