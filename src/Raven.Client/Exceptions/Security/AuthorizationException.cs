using System;

namespace Raven.Client.Exceptions.Security
{
    public class AuthorizationException : SecurityException
    {
        public AuthorizationException()
        {
        }

        public AuthorizationException(string message) : base(message)
        {
        }

        public AuthorizationException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}