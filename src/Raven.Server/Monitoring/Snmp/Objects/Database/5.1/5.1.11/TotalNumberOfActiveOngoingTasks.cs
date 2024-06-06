using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfActiveOngoingTasks : ActiveOngoingTasksBase
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
                    count += GetNumberOfActiveElasticSearchEtls(rachisState, nodeTag, database);
                    count += GetNumberOfActiveExternalReplications(rachisState, nodeTag, database);
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
    }
}
