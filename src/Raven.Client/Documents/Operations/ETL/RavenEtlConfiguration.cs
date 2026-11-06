using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.ETL
{
    public sealed class RavenEtlConfiguration : EtlConfiguration<RavenConnectionString>
    {
        private string _destination;

        public int? LoadRequestTimeoutInSec { get; set; }

        /// <summary>
        /// When enabled, the destination keeps a document as-is if the transformed result equals what it already stores,
        /// so its etag doesn't change and nothing downstream (indexes, subscriptions, revisions, replication, ETL) is triggered.
        /// </summary>
        /// <remarks>
        /// Applies only when the script loads into the same collection as the source document (or there is no script).
        /// Loads into a different collection, and scripts calling addAttachment, addCounter or addTimeSeries,
        /// delete the destination document before putting it, so those documents are always rewritten.
        /// Changes to <c>@collection</c>, <c>@expires</c>, <c>@refresh</c>, <c>@archive-at</c> and <c>@archived</c> metadata
        /// are still written; changes to other <c>@</c>-prefixed metadata alone are not.
        /// </remarks>
        public bool SkipOverwriteIfUnchanged { get; set; }

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

                if (ravenConfig.SkipOverwriteIfUnchanged != SkipOverwriteIfUnchanged)
                    diff |= EtlConfigurationCompareDifferences.ConfigurationOptions;
            }

            return diff;
        }

        public override DynamicJsonValue ToJson()
        {
            var result = base.ToJson();

            result[nameof(LoadRequestTimeoutInSec)] = LoadRequestTimeoutInSec;
            result[nameof(SkipOverwriteIfUnchanged)] = SkipOverwriteIfUnchanged;

            return result;
        }

        public override DynamicJsonValue ToAuditJson()
        {
            return ToJson();
        }
    }
}
