using System;

namespace Raven.Client.Exceptions.Security
{
    public class NameMismatchException : AuthenticationException
    {
        public NameMismatchException()
        {
        }

        public NameMismatchException(string message) : base(message)
        {
        }

        public NameMismatchException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
