using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfOngoingTasks : OngoingTasksBase
    {
        public TotalNumberOfOngoingTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfOngoingTasks)
        {
        }

        protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
        {
            var count = GetNumberOfElasticSearchEtls(database);
            count += GetNumberOfExternalReplications(database);
            count += GetNumberOfOlapEtls(database);
            count += GetNumberOfPeriodicBackups(database);
            count += GetNumberOfQueueEtls(database);
            count += GetNumberOfRavenEtls(database);
            count += GetNumberOfSinkPullReplications(database);
            count += GetNumberOfSqlEtls(database);
            count += GetNumberOfSubscriptions(context, database);

            return count;
        }
    }
}
