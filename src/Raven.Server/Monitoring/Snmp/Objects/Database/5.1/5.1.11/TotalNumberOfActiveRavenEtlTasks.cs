using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfActiveRavenEtlTasks : ActiveOngoingTasksBase
    {
        public TotalNumberOfActiveRavenEtlTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActiveRavenEtlTasks)
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
                    count += GetNumberOfActiveRavenEtls(rachisState, nodeTag, database);
                }
            }

            return new Integer32(count);
        }
    }
}
