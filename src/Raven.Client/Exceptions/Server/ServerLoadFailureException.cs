using System;

namespace Raven.Client.Exceptions.Server
{
    public class ServerLoadFailureException : RavenException
    {
        public ServerLoadFailureException()
        {
        }

        public ServerLoadFailureException(string message)
            : base(message)
        {
        }

        public ServerLoadFailureException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}