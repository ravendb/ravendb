using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfActiveBackupTasks : ActiveOngoingTasksBase
    {
        public TotalNumberOfActiveBackupTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActiveBackupTasks)
        {
        }

        protected override int GetCount(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
        {
            return GetNumberOfActivePeriodicBackups(rachisState, nodeTag, database);
        }
    }
}
