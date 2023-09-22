using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
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

        public static DatabaseLicenseLimitsUsage CreateFor<TTransaction>(TransactionOperationContext<TTransaction> context, RawDatabaseRecord databaseRecord)
            where TTransaction : RavenTransaction
        {
            var items = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.ItemsSchema, ClusterStateMachine.Items);

            var limits = new DatabaseLicenseLimitsUsage();

            limits.NumberOfStaticIndexes += databaseRecord.CountOfStaticIndexes;
            limits.NumberOfAutoIndexes += databaseRecord.CountOfAutoIndexes;
            limits.NumberOfCustomSorters += databaseRecord.CountOfSorters;
            limits.NumberOfAnalyzers += databaseRecord.CountOfAnalyzers;
            limits.NumberOfSubscriptions += ClusterStateMachine.GetSubscriptionsCountForDatabase(context.Allocator, items, databaseRecord.DatabaseName);

            return limits;
        }

        public static DatabaseLicenseLimitsUsage CreateFor<TTransaction>(TransactionOperationContext<TTransaction> context, ServerStore serverStore, string databaseName)
            where TTransaction : RavenTransaction
        {
            using (var databaseRecord = serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
                return CreateFor(context, databaseRecord);
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
