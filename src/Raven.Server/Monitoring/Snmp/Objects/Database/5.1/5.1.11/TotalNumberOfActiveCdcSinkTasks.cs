using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfActiveCdcSinkTasks : ActiveOngoingTasksBase
{
    public TotalNumberOfActiveCdcSinkTasks(ServerStore serverStore) : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActiveCdcSinkTasks)
    {
    }

    protected override int GetCount(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
    {
        return GetNumberOfActiveCdcSinks(rachisState, nodeTag, database);
    }
}
