using System;
using System.Runtime.Serialization;

namespace Raven.Abstractions.Exceptions
{
    [Serializable]
    public class ConcurrentLoadTimeoutException : Exception
    {
        //
        // For guidelines regarding the creation of new exception types, see
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/cpgenref/html/cpconerrorraisinghandlingguidelines.asp
        // and
        //    http://msdn.microsoft.com/library/default.asp?url=/library/en-us/dncscol/html/csharp07192001.asp
        //

        public ConcurrentLoadTimeoutException()
        {
        }

        public ConcurrentLoadTimeoutException(string message) : base(message)
        {
        }

        public ConcurrentLoadTimeoutException(string message, Exception inner) : base(message, inner)
        {
        }

        protected ConcurrentLoadTimeoutException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}
