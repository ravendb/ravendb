using System;
using System.Linq;
using Elasticsearch.Net;
using Nest;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    public static class ElasticSearchHelper
    {
        public static ElasticClient CreateClient(ElasticSearchConnectionString connectionString)
        {
            Uri[] nodesUrls = connectionString.Nodes.Select(x => new Uri(x)).ToArray();

            StaticConnectionPool pool = new StaticConnectionPool(nodesUrls);
            ConnectionSettings settings = new ConnectionSettings(pool);

            if (connectionString.Authentication.BasicAuth != null)
            {
                settings.BasicAuthentication(connectionString.Authentication.BasicAuth.Username, connectionString.Authentication.BasicAuth.Password);
            }
            else if (connectionString.Authentication.ApiKeyAuth != null)
            {
                settings.ApiKeyAuthentication("", connectionString.Authentication.ApiKeyAuth.ApiKey);
            }
            else if (connectionString.Authentication.CertificateAuth != null)
            {
                // add certificates
            }

            ElasticClient client = new(settings);

            return client;
        }
    }
}
