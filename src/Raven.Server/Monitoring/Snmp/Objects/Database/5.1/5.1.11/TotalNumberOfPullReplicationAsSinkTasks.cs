using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfPullReplicationAsSinkTasks : OngoingTasksBase
    {
        public TotalNumberOfPullReplicationAsSinkTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfPullReplicationAsSinkTasks)
        {
        }

        protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
        {
            return GetNumberOfSinkPullReplications(database);
        }
    }
}
