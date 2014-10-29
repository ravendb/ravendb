using System.Net;
using Raven.Abstractions.Connection;

namespace Raven.Client.Connection
{
    public class OperationMetadata
    {
        public OperationMetadata(string url, string username = null, string password = null, string domain = null, string apiKey = null)
        {
            Url = url;

            ICredentials credentials = null;
            if (!string.IsNullOrEmpty(username))
                credentials = new NetworkCredential(username, password ?? string.Empty, domain ?? string.Empty);

            Credentials = new OperationCredentials(apiKey, credentials);
        }

        public OperationMetadata(string url, ICredentials credentials, string apiKey = null)
        {
            Url = url;
            Credentials = new OperationCredentials(apiKey, credentials);
        }

        public OperationMetadata(string url, OperationCredentials credentials)
        {
            Url = url;
            Credentials = credentials != null ? new OperationCredentials(credentials.ApiKey, credentials.Credentials) : new OperationCredentials(null,null);
        }

        public OperationMetadata(OperationMetadata operationMetadata)
        {
            Url = operationMetadata.Url;
            Credentials = new OperationCredentials(operationMetadata.Credentials.ApiKey, operationMetadata.Credentials.Credentials);
        }

        public string Url { get; private set; }

        public OperationCredentials Credentials { get; private set; }
    }
}