using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfActiveQueueSinkTasks : ActiveOngoingTasksBase
{
    public TotalNumberOfActiveQueueSinkTasks(ServerStore serverStore) : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActiveQueueSinkTasks)
    {
    }

    protected override int GetCount(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
    {
        return GetNumberOfActiveQueueSinks(rachisState, nodeTag, database);
    }
}
