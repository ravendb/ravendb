using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfElasticSearchEtlTasks : OngoingTasksBase
    {
        public TotalNumberOfElasticSearchEtlTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfElasticSearchEtlTasks)
        {
        }

        protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
        {
            return GetNumberOfElasticSearchEtls(database);
        }
    }
}
