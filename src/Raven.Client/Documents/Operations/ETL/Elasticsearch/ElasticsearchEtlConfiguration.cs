using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.ElasticSearch
{
    public class ElasticSearchEtlConfiguration : EtlConfiguration<ElasticSearchConnectionString>
    {
        private string _destination;
        
        public ElasticSearchEtlConfiguration()
        {
            ElasticIndexes = new List<ElasticSearchIndex>();
        }

        public List<ElasticSearchIndex> ElasticIndexes { get; set; }
        
        public override string GetDestination()
        {
            return _destination ??= $"@{string.Join(",",Connection.Nodes)}";
        }

        public override EtlType EtlType => EtlType.ElasticSearch;
        
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
            return $"ElasticSearch ETL to {ConnectionStringName}";
        }
    }
    
    public class ElasticSearchIndex
    {
        public string IndexName { get; set; }
        public string IndexIdProperty { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(IndexName)] = IndexName,
            };
        }
    }
}
