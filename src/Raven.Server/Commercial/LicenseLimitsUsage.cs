using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public sealed class DatabaseLicenseLimitsUsage : IDynamicJson
    {
        public int NumberOfStaticIndexes { get; set; }

        public int NumberOfAutoIndexes { get; set; }

        public int NumberOfCustomSorters { get; set; }

        public int NumberOfAnalyzers { get; set; }

        public long NumberOfSubscriptions { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NumberOfStaticIndexes)] = NumberOfStaticIndexes,
                [nameof(NumberOfAutoIndexes)] = NumberOfAutoIndexes,
                [nameof(NumberOfCustomSorters)] = NumberOfCustomSorters,
                [nameof(NumberOfAnalyzers)] = NumberOfAnalyzers,
                [nameof(NumberOfSubscriptions)] = NumberOfSubscriptions
            };
        }
    }

    public sealed class LicenseLimitsUsage : IDynamicJson
    {
        public int NumberOfStaticIndexesInCluster { get; set; }

        public int NumberOfAutoIndexesInCluster { get; set; }

        public int NumberOfCustomSortersInCluster { get; set; }

        public int NumberOfAnalyzersInCluster { get; set; }

        public long NumberOfSubscriptionsInCluster { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NumberOfStaticIndexesInCluster)] = NumberOfStaticIndexesInCluster,
                [nameof(NumberOfAutoIndexesInCluster)] = NumberOfAutoIndexesInCluster,
                [nameof(NumberOfCustomSortersInCluster)] = NumberOfCustomSortersInCluster,
                [nameof(NumberOfAnalyzersInCluster)] = NumberOfAnalyzersInCluster,
                [nameof(NumberOfSubscriptionsInCluster)] = NumberOfSubscriptionsInCluster
            };
        }
    }
}
