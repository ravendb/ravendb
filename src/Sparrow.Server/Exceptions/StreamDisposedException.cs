using System;

namespace Sparrow.Server.Exceptions
{
    public class StreamDisposedException : Exception
    {
        public StreamDisposedException(string message) : base(message)
        {
        }
    }
}
