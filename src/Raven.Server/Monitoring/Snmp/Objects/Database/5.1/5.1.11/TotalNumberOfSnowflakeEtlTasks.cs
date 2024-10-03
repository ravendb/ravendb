using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfSnowflakeEtlTasks : OngoingTasksBase
{
    public TotalNumberOfSnowflakeEtlTasks(ServerStore serverStore)
        : base(serverStore, SnmpOids.Databases.General.TotalNumberOfSnowflakeEtlTasks)
    {
    }

    protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
    {
        return GetNumberOfSnowflakeEtls(database);
    }
}
