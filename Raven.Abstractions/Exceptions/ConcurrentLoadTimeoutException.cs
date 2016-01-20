using System;
using System.Runtime.Serialization;

namespace Raven.Abstractions.Exceptions
{
#if !DNXCORE50
    [Serializable]
#endif
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

#if !DNXCORE50
        protected ConcurrentLoadTimeoutException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
#endif
    }
}
