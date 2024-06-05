using System.Collections.Generic;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfActiveOngoingTasks : DatabaseBase<Integer32>
    {
        public TotalNumberOfActiveOngoingTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActiveOngoingTasks)
        {
        }

        protected override Integer32 GetData()
        {
            var count = 0;
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var rachisState = ServerStore.CurrentRachisState;
                var nodeTag = ServerStore.NodeTag;

                foreach (var database in GetDatabases(context))
                {
                    if (ServerStore.DatabasesLandlord.IsDatabaseLoaded(database.DatabaseName) == false)
                        continue;

                    count += GetNumberOfActiveElasticSearchEtls(rachisState, nodeTag, database);
                    count += GetNumberOfActiveExternalReplications(rachisState, nodeTag, database);
                    //count += GetNumberOfActiveHubPullReplications(rachisState, nodeTag, database);
                    count += GetNumberOfActiveOlapEtls(rachisState, nodeTag, database);
                    count += GetNumberOfActivePeriodicBackups(rachisState, nodeTag, database);
                    count += GetNumberOfActiveQueueEtls(rachisState, nodeTag, database);
                    count += GetNumberOfActiveRavenEtls(rachisState, nodeTag, database);
                    count += GetNumberOfActiveSinkPullReplications(rachisState, nodeTag, database);
                    count += GetNumberOfActiveSqlEtls(rachisState, nodeTag, database);
                    count += GetNumberOfActiveSubscriptions(context, rachisState, nodeTag, database);
                }
            }

            return new Integer32(count);
        }

        public static int GetNumberOfActiveElasticSearchEtls(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.ElasticSearchEtls.Where(x => x.Disabled == false));

        public static int GetNumberOfActiveExternalReplications(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.ExternalReplications.Where(x => x.Disabled == false));

        //public static int GetNumberOfActiveHubPullReplications(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.HubPullReplications.Where(x => x.Disabled == false));

        public static int GetNumberOfActiveOlapEtls(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.OlapEtls.Where(x => x.Disabled == false));

        public static int GetNumberOfActivePeriodicBackups(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.PeriodicBackups.Where(x => x.Disabled == false));

        public static int GetNumberOfActiveQueueEtls(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.QueueEtls.Where(x => x.Disabled == false));

        public static int GetNumberOfActiveRavenEtls(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.RavenEtls.Where(x => x.Disabled == false));

        public static int GetNumberOfActiveSinkPullReplications(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.SinkPullReplications.Where(x => x.Disabled == false));

        public static int GetNumberOfActiveSqlEtls(RachisState rachisState, string nodeTag, RawDatabaseRecord database) => CountTasks(rachisState, nodeTag, database.Topology, database.SqlEtls.Where(x => x.Disabled == false));

        public static int GetNumberOfActiveSubscriptions(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
        {
            var count = 0;
            foreach (var kvp in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(database.DatabaseName)))
            {
                var subscriptionState = JsonDeserializationClient.SubscriptionState(kvp.Value);
                if (subscriptionState.Disabled)
                    continue;

                var responsibleNode = database.Topology.WhoseTaskIsIt(rachisState, subscriptionState);
                if (responsibleNode != nodeTag)
                    continue;

                count++;
            }

            return count;
        }

        private static int CountTasks(RachisState rachisState, string nodeTag, DatabaseTopology databaseTopology, IEnumerable<IDatabaseTask> tasks)
        {
            if (tasks == null)
                return 0;

            var count = 0;
            foreach (var task in tasks)
            {
                var responsibleNode = databaseTopology.WhoseTaskIsIt(rachisState, task);
                if (responsibleNode != nodeTag)
                    continue;

                count++;
            }

            return count;
        }
    }
}
