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
        public static ElasticClient CreateClient(ElasticSearchConnectionString connectionString, TimeSpan? requestTimeout = null, TimeSpan? pingTimeout = null)
        {
            Uri[] nodesUrls = connectionString.Nodes.Select(x => new Uri(x)).ToArray();

            StaticConnectionPool pool = new StaticConnectionPool(nodesUrls);
            ConnectionSettings settings = new ConnectionSettings(pool);

            if (requestTimeout != null)
                settings.RequestTimeout(requestTimeout.Value);

            if (pingTimeout != null)
                settings.PingTimeout(pingTimeout.Value);

            if (connectionString.Authentication != null)
            {
                if (connectionString.Authentication.Basic != null)
                {
                    settings.BasicAuthentication(connectionString.Authentication.Basic.Username, connectionString.Authentication.Basic.Password);
                }
                else if (connectionString.Authentication.ApiKey != null)
                {
                    settings.ApiKeyAuthentication(connectionString.Authentication.ApiKey.ApiKeyId, connectionString.Authentication.ApiKey.ApiKey);
                }
                else if (connectionString.Authentication.Certificate != null)
                {
                    if (connectionString.Authentication.Certificate.CertificatesBase64.Length == 1)
                    {
                        var cert = new X509Certificate2(Convert.FromBase64String(connectionString.Authentication.Certificate.CertificatesBase64.First()));
                        settings.ClientCertificate(cert);
                    }
                    else
                    {
                        var certificates = new X509CertificateCollection();

                        foreach (var certificateBase64 in connectionString.Authentication.Certificate.CertificatesBase64)
                        {
                            certificates.Add(new X509Certificate2(Convert.FromBase64String(certificateBase64)));
                        }

                        settings.ClientCertificates(certificates);
                    }
                }
            }

            ElasticClient client = new(settings);

            return client;
        }
    }
}
