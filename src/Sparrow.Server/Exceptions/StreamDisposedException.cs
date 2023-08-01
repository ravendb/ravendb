using System;

namespace Sparrow.Server.Exceptions
{
    public sealed class StreamDisposedException : Exception
    {
        public StreamDisposedException(string message) : base(message)
        {
        }
    }
}
