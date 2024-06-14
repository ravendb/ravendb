using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfQueueSinkTasks : OngoingTasksBase
{
    public TotalNumberOfQueueSinkTasks(ServerStore serverStore)
        : base(serverStore, SnmpOids.Databases.General.TotalNumberOfQueueSinkTasks)
    {
    }

    protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
    {
        return GetNumberOfQueueSinks(database);
    }
}
