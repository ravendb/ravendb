using System;
using Raven.Client.Server.Tcp;

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

        public static AuthorizationException Forbidden(string url)
        {
            return new AuthorizationException($"Forbidden access to {url}. Make sure you're using the correct certificate.");
        }
    }
}