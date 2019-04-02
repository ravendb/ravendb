using System;

namespace Raven.Server.Exceptions
{
    public class KeyTooBigException : Exception
    {
        public KeyTooBigException(string message)
            : base(message)
        {
        }
    }
}
