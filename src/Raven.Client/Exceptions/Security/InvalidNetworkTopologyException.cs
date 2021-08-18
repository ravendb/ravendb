using System;

namespace Raven.Client.Exceptions.Security
{
    public class InvalidNetworkTopologyException : SecurityException
    {
        public InvalidNetworkTopologyException()
        {
        }

        public InvalidNetworkTopologyException(string message) : base(message)
        {
        }

        public InvalidNetworkTopologyException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
