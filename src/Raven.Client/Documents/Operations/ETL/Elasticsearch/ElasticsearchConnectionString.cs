using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.ElasticSearch
{
    public sealed class ElasticSearchConnectionString : ConnectionString
    {
        public string[] Nodes;

        public Authentication Authentication;

        public override ConnectionStringType Type => ConnectionStringType.ElasticSearch;
        
        [Obsolete("Elasticsearch compatibility isn't required anymore to connect with Elasticsearch server v8.x.")]
        public bool EnableCompatibilityMode { get; set; }

        protected override void ValidateImpl(ref List<string> errors)
        {
            if (Nodes == null || Nodes.Length == 0)
                errors.Add($"{nameof(Nodes)} cannot be empty");

            if (Nodes == null)
                return;

            for (int i = 0; i < Nodes.Length; i++)
            {
                if (Nodes[i] == null)
                {
                    errors.Add($"Url number {i + 1} in {nameof(Nodes)} cannot be empty");
                    continue;
                }

                Nodes[i] = Nodes[i].Trim();
            }
        }

        public override bool IsEqual(ConnectionString connectionString)
        {
            if (connectionString is ElasticSearchConnectionString elasticConnection)
            {
                if (Nodes.Length != elasticConnection.Nodes.Length)
                    return false;

                foreach (var url in Nodes)
                {
                    if (elasticConnection.Nodes.Contains(url) == false)
                        return false;
                }

                var isEqual = base.IsEqual(connectionString);
                return isEqual && Nodes.SequenceEqual(elasticConnection.Nodes);
            }

            return false;
        }

        public override DynamicJsonValue ToJson()
        {
            DynamicJsonValue json = base.ToJson();
            json[nameof(Nodes)] = new DynamicJsonArray(Nodes);
#pragma warning disable CS0618 // Type or member is obsolete
            json[nameof(EnableCompatibilityMode)] = EnableCompatibilityMode;
#pragma warning restore CS0618 // Type or member is obsolete
            json[nameof(Authentication)] = Authentication == null ? null : new DynamicJsonValue()
            {
                [nameof(Authentication.Basic)] = Authentication.Basic == null ? null : new DynamicJsonValue()
                {
                    [nameof(Authentication.Basic.Username)] = Authentication?.Basic?.Username,
                    [nameof(Authentication.Basic.Password)] = Authentication?.Basic?.Password
                },
                [nameof(Authentication.ApiKey)] = Authentication.ApiKey == null ? null : new DynamicJsonValue()
                {
                    [nameof(Authentication.ApiKey.ApiKeyId)] = Authentication?.ApiKey?.ApiKeyId,
                    [nameof(Authentication.ApiKey.ApiKey)] = Authentication?.ApiKey?.ApiKey,
                    [nameof(Authentication.ApiKey.EncodedApiKey)] = Authentication?.ApiKey?.EncodedApiKey
                },
                [nameof(Authentication.Certificate)] = Authentication.Certificate == null ? null : new DynamicJsonValue()
                {
                    [nameof(Authentication.Certificate.CertificatesBase64)] = new DynamicJsonArray(Authentication?.Certificate?.CertificatesBase64)
                },
            };

            return json;
        }

        public override DynamicJsonValue ToAuditJson()
        {
            DynamicJsonValue json = base.ToAuditJson();
            
            json[nameof(Nodes)] = new DynamicJsonArray(Nodes);
#pragma warning disable CS0618 // Type or member is obsolete
            json[nameof(EnableCompatibilityMode)] = EnableCompatibilityMode;
#pragma warning restore CS0618 // Type or member is obsolete

            return json;
        }
    }
    
    public sealed class Authentication
    {
        public ApiKeyAuthentication ApiKey { get; set; }
        public BasicAuthentication Basic { get; set; }
        public CertificateAuthentication Certificate { get; set; }
    }

    public sealed class ApiKeyAuthentication
    {
        public string ApiKeyId { get; set; }
        
        public string ApiKey { get; set; }
        public string EncodedApiKey { get; set; }
    }
    
    public sealed class BasicAuthentication
    {
        public string Username { get; set; }
        public string Password { get; set; }
    }
    
    public sealed class CertificateAuthentication
    {
        public string[] CertificatesBase64 { get; set; }
    }
}
