using System;

namespace Raven.Client.Exceptions.Cluster
{
    public class CommandExecutionException : RavenException
    {
        public CommandExecutionException()
        {
        }

        public CommandExecutionException(string message)
            : base(message)
        {
        }

        public CommandExecutionException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
