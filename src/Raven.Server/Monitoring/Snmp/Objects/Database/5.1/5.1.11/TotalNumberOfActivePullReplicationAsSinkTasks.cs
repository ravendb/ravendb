using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfActivePullReplicationAsSinkTasks : ActiveOngoingTasksBase
    {
        public TotalNumberOfActivePullReplicationAsSinkTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActivePullReplicationAsSinkTasks)
        {
        }

        protected override int GetCount(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
        {
            return GetNumberOfActiveSinkPullReplications(rachisState, nodeTag, database);
        }
    }
}
