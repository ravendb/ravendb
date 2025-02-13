using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfAiIntegrationTasks : OngoingTasksBase
{
    public TotalNumberOfAiIntegrationTasks(ServerStore serverStore) 
        : base(serverStore, SnmpOids.Databases.General.TotalNumberOfAiIntegrationTasks)
    {
    }

    protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
    {
        return GetNumberOfAiIntegrations(database);
    }
}
