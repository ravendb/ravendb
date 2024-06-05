using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfOngoingTasks : DatabaseBase<Integer32>
    {
        public TotalNumberOfOngoingTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfOngoingTasks)
        {
        }

        protected override Integer32 GetData()
        {
            var count = 0;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var database in GetDatabases(context))
                {
                    count += GetNumberOfElasticSearchEtls(database);
                    count += GetNumberOfExternalReplications(database);
                    //count += GetNumberOfHubPullReplications(database);
                    count += GetNumberOfOlapEtls(database);
                    count += GetNumberOfPeriodicBackups(database);
                    count += GetNumberOfQueueEtls(database);
                    count += GetNumberOfRavenEtls(database);
                    count += GetNumberOfSinkPullReplications(database);
                    count += GetNumberOfSqlEtls(database);
                    count += GetNumberOfSubscriptions(context, database);
                }
            }

            return new Integer32(count);
        }

        public static int GetNumberOfElasticSearchEtls(RawDatabaseRecord database) => database.ElasticSearchEtls?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfExternalReplications(RawDatabaseRecord database) => database.ExternalReplications?.Count(x => x.Disabled == false) ?? 0;

        //public static int GetNumberOfHubPullReplications(RawDatabaseRecord database) => database.HubPullReplications?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfOlapEtls(RawDatabaseRecord database) => database.OlapEtls?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfPeriodicBackups(RawDatabaseRecord database) => database.PeriodicBackups?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfQueueEtls(RawDatabaseRecord database) => database.QueueEtls?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfRavenEtls(RawDatabaseRecord database) => database.RavenEtls?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfSinkPullReplications(RawDatabaseRecord database) => database.SinkPullReplications?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfSqlEtls(RawDatabaseRecord database) => database.SqlEtls?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfSubscriptions(TransactionOperationContext context, RawDatabaseRecord database)
        {
            var count = 0;
            foreach (var kvp in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(database.DatabaseName)))
            {
                if (kvp.Value.TryGet(nameof(SubscriptionState.Disabled), out bool disabled) && disabled)
                    continue;

                count++;
            }

            return count;
        }
    }
}
