using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Subscriptions;
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
    }
}
