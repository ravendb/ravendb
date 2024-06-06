using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfActiveSqlEtlTasks : ActiveOngoingTasksBase
    {
        public TotalNumberOfActiveSqlEtlTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActiveSqlEtlTasks)
        {
        }

        protected override int GetCount(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
        {
            return GetNumberOfActiveSqlEtls(rachisState, nodeTag, database);
        }
    }
}
