using System;

namespace Raven.Client.Exceptions
{
    public class StreamDisposedException : Exception
    {
        public StreamDisposedException(string message) : base(message)
        {
        }
    }
}
