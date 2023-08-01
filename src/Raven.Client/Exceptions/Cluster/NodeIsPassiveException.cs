using System;

namespace Raven.Client.Exceptions.Cluster
{
    public sealed class NodeIsPassiveException : RavenException
    {
        public NodeIsPassiveException()
        {
        }

        public NodeIsPassiveException(string message) : base(message)
        {
        }

        public NodeIsPassiveException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
