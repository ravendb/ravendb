using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfRavenEtlTasks : OngoingTasksBase
    {
        public TotalNumberOfRavenEtlTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfRavenEtlTasks)
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
                    count += GetNumberOfRavenEtls(database);
                }
            }

            return new Integer32(count);
        }
    }
}
