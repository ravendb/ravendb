using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfActiveSnowflakeTasks : ActiveOngoingTasksBase
{
    public TotalNumberOfActiveSnowflakeTasks(ServerStore serverStore) : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActiveSnowflakeEtlTasks)
    {
    }

    protected override int GetCount(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
    {
        return GetNumberOfActiveSnowflakeEtls(rachisState, nodeTag, database);
    }
}
