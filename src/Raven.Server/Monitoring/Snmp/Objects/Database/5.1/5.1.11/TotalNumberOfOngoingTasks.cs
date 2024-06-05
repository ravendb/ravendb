// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

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
                    count += GetNumberOfActiveElasticSearchEtls(database);
                    count += GetNumberOfActiveExternalReplications(database);
                    //count += GetNumberOfActiveHubPullReplications(database);
                    count += GetNumberOfActiveOlapEtls(database);
                    count += GetNumberOfActivePeriodicBackups(database);
                    count += GetNumberOfActiveQueueEtls(database);
                    count += GetNumberOfActiveRavenEtls(database);
                    count += GetNumberOfActiveSinkPullReplications(database);
                    count += GetNumberOfActiveSqlEtls(database);
                    count += GetNumberOfActiveSubscriptions(context, database);
                }
            }

            return new Integer32(count);
        }

        public static int GetNumberOfActiveElasticSearchEtls(RawDatabaseRecord database) => database.ElasticSearchEtls?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfActiveExternalReplications(RawDatabaseRecord database) => database.ExternalReplications?.Count(x => x.Disabled == false) ?? 0;

        //public static int GetNumberOfActiveHubPullReplications(RawDatabaseRecord database) => database.HubPullReplications?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfActiveOlapEtls(RawDatabaseRecord database) => database.OlapEtls?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfActivePeriodicBackups(RawDatabaseRecord database) => database.PeriodicBackups?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfActiveQueueEtls(RawDatabaseRecord database) => database.QueueEtls?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfActiveRavenEtls(RawDatabaseRecord database) => database.RavenEtls?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfActiveSinkPullReplications(RawDatabaseRecord database) => database.SinkPullReplications?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfActiveSqlEtls(RawDatabaseRecord database) => database.SqlEtls?.Count(x => x.Disabled == false) ?? 0;

        public static int GetNumberOfActiveSubscriptions(TransactionOperationContext context, RawDatabaseRecord database)
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
