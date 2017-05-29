using System;

namespace Sparrow.Exceptions
{
    public class StreamDisposedException : Exception
    {
        public StreamDisposedException(string message) : base(message)
        {
        }
    }
}
