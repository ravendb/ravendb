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
            return new AuthorizationException($"Forbidden access to {url}. Make sure you're using the correct api-key.");
        }

        public static Exception EmptyApiKey(string url)
        {
            return new AuthorizationException($"Got unauthorized response for {url}. Please specify an api-key.");
        }

        public static Exception Unauthorized(TcpConnectionHeaderResponse.AuthorizationStatus status, string dbName)
        {
            return new AuthorizationException($"Got unauthorized response ({status}) for TCP connection to {dbName}");
        }

        public static Exception Unauthorized(string url)
        {
            return new AuthorizationException($"Got unauthorized response for {url} after trying to authenticate using specified api-key.");
        }
    }
}