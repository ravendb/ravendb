using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfSqlEtlTasks : OngoingTasksBase
    {
        public TotalNumberOfSqlEtlTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfSqlEtlTasks)
        {
        }

        protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
        {
            return GetNumberOfSqlEtls(database);
        }
    }
}
