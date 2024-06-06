using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfActiveQueueEtlTasks : ActiveOngoingTasksBase
    {
        public TotalNumberOfActiveQueueEtlTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActiveQueueEtlTasks)
        {
        }

        protected override int GetCount(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
        {
            return GetNumberOfActiveQueueEtls(rachisState, nodeTag, database);
        }
    }
}
