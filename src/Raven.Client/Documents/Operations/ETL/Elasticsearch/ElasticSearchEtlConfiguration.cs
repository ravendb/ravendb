using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL.ElasticSearch
{
    public sealed class ElasticSearchEtlConfiguration : EtlConfiguration<ElasticSearchConnectionString>
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

        internal override EtlConfigurationCompareDifferences Compare(EtlConfiguration<ElasticSearchConnectionString> config, Dictionary<string, ElasticSearchConnectionString> connectionStrings, List<(string TransformationName, EtlConfigurationCompareDifferences Difference)> transformationDiffs = null)
        {
            var diff = base.Compare(config, connectionStrings, transformationDiffs);

            if (config is ElasticSearchEtlConfiguration elasticConfig)
            {
                if (EnumerableExtension.ContentEqualsIgnoringOrder(elasticConfig.ElasticIndexes, ElasticIndexes) == false)
                    diff |= EtlConfigurationCompareDifferences.ConfigurationOptions;
            }

            return diff;
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(ElasticIndexes)] = ElasticIndexes != null ? new DynamicJsonArray(ElasticIndexes.Select(x => x.ToJson())) : null;

            return json;
        }

        public override DynamicJsonValue ToAuditJson()
        {
            return ToJson();
        }
    }
    
    public class ElasticSearchIndex
    {
        public string IndexName { get; set; }

        public string DocumentIdProperty { get; set; }

        public bool InsertOnlyMode { get; set; }

        private bool Equals(ElasticSearchIndex other)
        {
            return string.Equals(IndexName, other.IndexName) && string.Equals(DocumentIdProperty, other.DocumentIdProperty) &&
                   InsertOnlyMode == other.InsertOnlyMode;
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(this, obj) || obj is ElasticSearchIndex other && Equals(other);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(IndexName, DocumentIdProperty, InsertOnlyMode);
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(IndexName)] = IndexName,
                [nameof(DocumentIdProperty)] = DocumentIdProperty,
                [nameof(InsertOnlyMode)] = InsertOnlyMode
            };
        }
    }
}
