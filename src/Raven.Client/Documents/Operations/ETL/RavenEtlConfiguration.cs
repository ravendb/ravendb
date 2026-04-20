using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL
{
    public sealed class RavenEtlConfiguration : EtlConfiguration<RavenConnectionString>
    {
        private string _destination;

        public int? LoadRequestTimeoutInSec { get; set; }

        public override EtlType EtlType => EtlType.Raven;

        public override string GetDestination()
        {
            return _destination ?? (_destination = $"{Connection.Database}@{string.Join(",",Connection.TopologyDiscoveryUrls)}");
        }

        public override bool UsingEncryptedCommunicationChannel()
        {
            foreach (var url in Connection.TopologyDiscoveryUrls)
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
            return $"RavenDB ETL to {ConnectionStringName}";
        }

        internal override EtlConfigurationCompareDifferences Compare(EtlConfiguration<RavenConnectionString> config, Dictionary<string, RavenConnectionString> connectionStrings, List<(string TransformationName, EtlConfigurationCompareDifferences Difference)> transformationDiffs = null)
        {
            var diff = base.Compare(config, connectionStrings, transformationDiffs);

            if (config is RavenEtlConfiguration ravenConfig)
            {
                if (ravenConfig.LoadRequestTimeoutInSec != LoadRequestTimeoutInSec)
                    diff |= EtlConfigurationCompareDifferences.ConfigurationOptions;
            }

            return diff;
        }

        public override DynamicJsonValue ToJson()
        {
            var result = base.ToJson();

            result[nameof(LoadRequestTimeoutInSec)] = LoadRequestTimeoutInSec;

            return result;
        }

        public override DynamicJsonValue ToAuditJson()
        {
            return ToJson();
        }
    }
}
