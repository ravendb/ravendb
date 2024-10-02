using Raven.Client.ServerWide;
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

        protected override int GetCount(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
        {
            var count = GetNumberOfActiveElasticSearchEtls(rachisState, nodeTag, database);
            count += GetNumberOfActiveExternalReplications(rachisState, nodeTag, database);
            count += GetNumberOfActiveOlapEtls(rachisState, nodeTag, database);
            count += GetNumberOfActivePeriodicBackups(rachisState, nodeTag, database);
            count += GetNumberOfActiveQueueEtls(rachisState, nodeTag, database);
            count += GetNumberOfActiveRavenEtls(rachisState, nodeTag, database);
            count += GetNumberOfActiveSinkPullReplications(rachisState, nodeTag, database);
            count += GetNumberOfActiveSqlEtls(rachisState, nodeTag, database);
            count += GetNumberOfActiveSubscriptions(context, rachisState, nodeTag, database);

            return count;
        }
    }
}
