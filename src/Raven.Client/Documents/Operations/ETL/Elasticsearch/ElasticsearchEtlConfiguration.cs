using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.Elasticsearch
{
    public class ElasticsearchEtlConfiguration : EtlConfiguration<ElasticsearchConnectionString>
    {
        private string _destination;
        
        public ElasticsearchEtlConfiguration()
        {
            ElasticIndexes = new List<ElasticsearchIndex>();
        }
        
        public List<ElasticsearchIndex> ElasticIndexes { get; set; }
        
        public override string GetDestination()
        {
            return _destination ??= $"@{string.Join(",",Connection.Nodes)}";
        }

        public override EtlType EtlType => EtlType.Elasticsearch;
        
        public override bool UsingEncryptedCommunicationChannel()
        {
            foreach (var url in Connection.Nodes)
            {
                if (url.StartsWith("http:", StringComparison.OrdinalIgnoreCase))
                {
                    return false;
                }
            }
            return true;
        }

        public override string GetDefaultTaskName()
        {
            return $"Elasticsearch ETL to {ConnectionStringName}";
        }
    }
    
    public class ElasticsearchIndex
    {
        public string IndexName { get; set; }
        public string IndexIdProperty { get; set; }

        protected bool Equals(ElasticsearchIndex other)
        {
            return string.Equals(IndexName, other.IndexName, StringComparison.OrdinalIgnoreCase) &&
                   string.Equals(IndexIdProperty, other.IndexIdProperty, StringComparison.OrdinalIgnoreCase);
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(IndexName)] = IndexName,
            };
        }
    }
}
