using System;

namespace Raven.Client.Exceptions
{
    public class RequestedNodeUnavailableException : Exception
    {
        public RequestedNodeUnavailableException()
        {
        }

        public RequestedNodeUnavailableException(string message) : base(message)
        {
        }

        public RequestedNodeUnavailableException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
