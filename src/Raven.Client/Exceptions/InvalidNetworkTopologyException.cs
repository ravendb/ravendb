using System;

namespace Raven.Client.Exceptions
{
    public sealed class InvalidNetworkTopologyException : Exception
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
