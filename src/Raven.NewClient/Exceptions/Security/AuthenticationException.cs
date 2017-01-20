using System;

namespace Raven.NewClient.Exceptions.Security
{
    public class AuthenticationException : SecurityException
    {
        public AuthenticationException()
        {
        }

        public AuthenticationException(string message) : base(message)
        {
        }

        public AuthenticationException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}