using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfActiveAiEtlTasks : ActiveOngoingTasksBase
{
    public TotalNumberOfActiveAiEtlTasks(ServerStore serverStore) 
        : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActiveAiEtlTasks)
    {
    }
    protected override int GetCount(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
    {
        return GetNumberOfActiveAiEtls(rachisState, nodeTag, database);
    }
}
