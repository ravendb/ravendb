using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public sealed class LicenseLimitsUsage : IDynamicJson
    {
        public int? ClusterStaticIndexes { get; set; }

        public int? ClusterAutoIndexes { get; set; }

        public int? ClusterSubscriptionTasks { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ClusterStaticIndexes)] = ClusterStaticIndexes,
                [nameof(ClusterAutoIndexes)] = ClusterAutoIndexes,
                [nameof(ClusterSubscriptionTasks)] = ClusterSubscriptionTasks
            };
        }
    }
}
