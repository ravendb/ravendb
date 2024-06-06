using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfExternalReplicationTasks : OngoingTasksBase
    {
        public TotalNumberOfExternalReplicationTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfExternalReplicationTasks)
        {
        }

        protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
        {
            return GetNumberOfExternalReplications(database);
        }
    }
}
