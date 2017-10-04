using System.Net;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Connection;

namespace Raven.Client.Connection
{
    public class OperationMetadata
    {
        internal OperationMetadata()
        {
        }

        public OperationMetadata(string url, string username = null, string password = null, string domain = null, string apiKey = null, ClusterInformation clusterInformation = null)
        {
            Url = url;

            ICredentials credentials = null;
            if (!string.IsNullOrEmpty(username))
                credentials = new NetworkCredential(username, password ?? string.Empty, domain ?? string.Empty);

            Credentials = new OperationCredentials(apiKey, credentials);
            ClusterInformation = clusterInformation != null ? new ClusterInformation(clusterInformation.IsInCluster, clusterInformation.IsLeader) : ClusterInformation.NotInCluster;
        }

        public OperationMetadata(string url, ICredentials credentials, string apiKey = null)
        {
            Url = url;
            Credentials = new OperationCredentials(apiKey, credentials);
        }

        public OperationMetadata(string url, OperationCredentials credentials, ClusterInformation clusterInformation)
        {
            Url = url;
            Credentials = credentials != null ? new OperationCredentials(credentials.ApiKey, credentials.Credentials) : new OperationCredentials(null,null);
            ClusterInformation = clusterInformation != null ? new ClusterInformation(clusterInformation.IsInCluster, clusterInformation.IsLeader) : ClusterInformation.NotInCluster;
        }

        public OperationMetadata(OperationMetadata operationMetadata)
        {
            Url = operationMetadata.Url;
            Credentials = new OperationCredentials(operationMetadata.Credentials.ApiKey, operationMetadata.Credentials.Credentials);
            ClusterInformation = new ClusterInformation(operationMetadata.ClusterInformation.IsInCluster, operationMetadata.ClusterInformation.IsLeader);
        }

        public string Url { get; private set; }

        public ClusterInformation ClusterInformation { get; private set; }

        public OperationCredentials Credentials { get; private set; }

        protected bool Equals(OperationMetadata other)
        {
            return string.Equals(Url, other.Url) && Equals(ClusterInformation, other.ClusterInformation) && Equals(Credentials, other.Credentials);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }
            if (ReferenceEquals(this, obj))
            {
                return true;
            }
            if (obj.GetType() != GetType())
            {
                return false;
            }
            return Equals((OperationMetadata)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (Url != null ? Url.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (ClusterInformation != null ? ClusterInformation.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Credentials != null ? Credentials.GetHashCode() : 0);
                return hashCode;
            }
        }

        public override string ToString()
        {
            return $"{Url} IsLeader={ClusterInformation.IsLeader}";
        }
    }
}
