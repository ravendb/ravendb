using System;
using Raven.Client.Http;

namespace Raven.Client.Exceptions
{
    public class AllTopologyNodesDownException : Exception
    {
        public Topology FailedTopology { get; }

        public AllTopologyNodesDownException()
        {
        }

        public AllTopologyNodesDownException(string message) : base(message)
        {
        }

        public AllTopologyNodesDownException(string message, Exception innerException) : base(message, innerException)
        {
        }

        public AllTopologyNodesDownException(string message, Topology failedTopology, Exception innerException) : base(message, innerException)
        {
            FailedTopology = failedTopology;
        }

    }
}
