using System;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
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
                settings.ApiKeyAuthentication(connectionString.Authentication.ApiKeyAuth.ApiKeyId, connectionString.Authentication.ApiKeyAuth.ApiKey);
            }
            else if (connectionString.Authentication.CertificateAuth != null)
            {
                if (connectionString.Authentication.CertificateAuth.CertificatesBase64.Length == 1)
                {
                    var cert = new X509Certificate2(Convert.FromBase64String(connectionString.Authentication.CertificateAuth.CertificatesBase64.First()));
                    settings.ClientCertificate(cert);
                }
                else
                {
                    var certificates = new X509CertificateCollection();

                    foreach (var certificateBase64 in connectionString.Authentication.CertificateAuth.CertificatesBase64)
                    {
                        certificates.Add(new X509Certificate2(Convert.FromBase64String(certificateBase64)));
                    }

                    settings.ClientCertificates(certificates);
                }
            }

            ElasticClient client = new(settings);

            return client;
        }
    }
}
