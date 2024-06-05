using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfExternalReplicationTasks : DatabaseBase<Integer32>
    {
        public TotalNumberOfExternalReplicationTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfExternalReplicationTasks)
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
                    count += TotalNumberOfOngoingTasks.GetNumberOfActiveExternalReplications(database);
                }
            }

            return new Integer32(count);
        }
    }
}
