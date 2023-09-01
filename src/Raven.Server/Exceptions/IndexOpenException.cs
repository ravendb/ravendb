using System;

namespace Raven.Server.Exceptions
{
    public sealed class IndexOpenException : Exception
    {
        public IndexOpenException(string message)
            : base(message)
        {
        }

        public IndexOpenException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}
