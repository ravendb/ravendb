using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Elasticsearch
{
    public class ElasticsearchConnectionString : ConnectionString
    {
        public string[] Nodes;

        public override ConnectionStringType Type => ConnectionStringType.Elasticsearch;

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
            if (connectionString is ElasticsearchConnectionString elasticConnection)
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
}
