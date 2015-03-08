using System.Net;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Connection;

namespace Raven.Client.Connection
{
    public class OperationMetadata
    {
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
    }
}