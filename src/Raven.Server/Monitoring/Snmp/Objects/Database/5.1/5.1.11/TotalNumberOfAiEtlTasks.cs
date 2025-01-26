using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfAiEtlTasks : OngoingTasksBase
{
    public TotalNumberOfAiEtlTasks(ServerStore serverStore) 
        : base(serverStore, SnmpOids.Databases.General.TotalNumberOfAiEtlTasks)
    {
    }

    protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
    {
        return GetNumberOfAiEtls(database);
    }
}
