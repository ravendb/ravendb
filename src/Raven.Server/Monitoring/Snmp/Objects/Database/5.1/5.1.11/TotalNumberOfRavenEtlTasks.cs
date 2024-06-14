using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfRavenEtlTasks : OngoingTasksBase
    {
        public TotalNumberOfRavenEtlTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfRavenEtlTasks)
        {
        }

        protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
        {
            return GetNumberOfRavenEtls(database);
        }
    }
}
