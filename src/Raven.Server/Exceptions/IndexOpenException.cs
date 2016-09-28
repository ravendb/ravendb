using System;

namespace Raven.Server.Exceptions
{
    public class IndexOpenException : Exception
    {
        public IndexOpenException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}