using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfActiveRavenEtlTasks : ActiveOngoingTasksBase
    {
        public TotalNumberOfActiveRavenEtlTasks(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActiveRavenEtlTasks)
        {
        }

        protected override int GetCount(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
        {
            return GetNumberOfActiveRavenEtls(rachisState, nodeTag, database);
        }
    }
}
