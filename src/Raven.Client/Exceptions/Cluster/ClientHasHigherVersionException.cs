using System;

namespace Raven.Client.Exceptions.Cluster
{
    public class ClientHasHigherVersionException : RavenException
    {
        public ClientHasHigherVersionException()
        {
        }

        public ClientHasHigherVersionException(string message) : base(message)
        {
        }

        public ClientHasHigherVersionException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
