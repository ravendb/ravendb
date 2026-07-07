using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfCdcSinkTasks : OngoingTasksBase
{
    public TotalNumberOfCdcSinkTasks(ServerStore serverStore)
        : base(serverStore, SnmpOids.Databases.General.TotalNumberOfCdcSinkTasks)
    {
    }

    protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
    {
        return GetNumberOfCdcSinks(database);
    }
}
