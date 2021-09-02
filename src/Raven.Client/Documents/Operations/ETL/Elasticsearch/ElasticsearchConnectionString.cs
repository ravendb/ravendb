using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.ElasticSearch
{
    public class ElasticSearchConnectionString : ConnectionString
    {
        public string[] Nodes;

        public Authentication Authentication;

        public override ConnectionStringType Type => ConnectionStringType.ElasticSearch;

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

            return json;
        }
    }
    
    public class Authentication
    {
        public ApiKeyAuth ApiKeyAuth { get; set; }
        public BasicAuth BasicAuth { get; set; }
        public CertificateAuth CertificateAuth { get; set; }
    }

    public class ApiKeyAuth
    {
        public string ApiKeyId { get; set; }
        
        public string ApiKey { get; set; }
    }
    
    public class BasicAuth
    {
        public string Username { get; set; }
        public string Password { get; set; }
    }
    
    public class CertificateAuth
    {
        public string[] CertificatesBase64 { get; set; }
    }
}
