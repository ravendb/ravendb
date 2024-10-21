using System;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Util;
using Sparrow;
using BasicAuthentication = Elastic.Transport.BasicAuthentication;

namespace Raven.Server.Documents.ETL.Providers.ElasticSearch
{
    public static class ElasticSearchHelper
    {
        public static ElasticsearchClient CreateClient(ElasticSearchConnectionString connectionString, TimeSpan? requestTimeout = null, TimeSpan? pingTimeout = null, bool useCustomBlittableSerializer = true)
        {
            Uri[] nodesUrls = connectionString.Nodes.Select(x => new Uri(x)).ToArray();

            var pool = new StaticNodePool(nodesUrls);
            var settings = useCustomBlittableSerializer
                ? new ElasticsearchClientSettings(pool, sourceSerializer: (@in, values) => new BlittableJsonElasticSerializer())
                : new ElasticsearchClientSettings(pool);

            if (requestTimeout != null)
                settings.RequestTimeout(requestTimeout.Value);

            if (pingTimeout != null)
                settings.PingTimeout(pingTimeout.Value);

            if (connectionString.Authentication != null)
            {
                if (connectionString.Authentication.Basic != null)
                {
                    settings.Authentication(new BasicAuthentication(connectionString.Authentication.Basic.Username, connectionString.Authentication.Basic.Password));
                }
                else if (connectionString.Authentication.ApiKey != null)
                {
                    if (connectionString.Authentication.ApiKey.EncodedApiKey != null)
                    {
                        settings.Authentication(new ApiKey(connectionString.Authentication.ApiKey.EncodedApiKey));
                    }
                    else
                    { 
                        var apiKeyMergedBytes = Encodings.Utf8.GetBytes($"{connectionString.Authentication.ApiKey.ApiKeyId}:{connectionString.Authentication.ApiKey.ApiKey}");
                        var encodedApiKey = Convert.ToBase64String(apiKeyMergedBytes);
                        settings.Authentication(new ApiKey(encodedApiKey));    
                    }
                    
                }
                else if (connectionString.Authentication.Certificate != null)
                {
                    if (connectionString.Authentication.Certificate.CertificatesBase64.Length == 1)
                    {
                        var cert = CertificateLoaderUtil.CreateCertificateFromAny(Convert.FromBase64String(connectionString.Authentication.Certificate.CertificatesBase64.First()));
                        settings.ClientCertificate(cert);
                    }
                    else
                    {
                        var certificates = new X509CertificateCollection();

                        foreach (var certificateBase64 in connectionString.Authentication.Certificate.CertificatesBase64)
                        {
                            certificates.Add(CertificateLoaderUtil.CreateCertificateFromAny(Convert.FromBase64String(certificateBase64)));
                        }

                        settings.ClientCertificates(certificates);
                    }
                }
            }

            ElasticsearchClient client = new(settings);

            return client;
        }
    }
}
