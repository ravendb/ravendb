namespace Raven.Server.Commercial
{
    public sealed class LicenseLimitsUsage
    {
        public int? ClusterStaticIndexes { get; set; }

        public int? ClusterAutoIndexes { get; set; }

        public int? ClusterSubscriptionTasks { get; set; }
    }
}
