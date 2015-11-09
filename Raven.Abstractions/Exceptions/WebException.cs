#if DNXCORE50
using System;

namespace Raven.Abstractions.Exceptions
{
    public class WebException : Exception
    {
        public WebException()
        {
        }

        public WebException(string message) : base(message)
        {
        }

        public WebException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
#endif