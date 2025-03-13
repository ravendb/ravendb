using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfActiveEmbeddingsGenerationTasks : ActiveOngoingTasksBase
{
    public TotalNumberOfActiveEmbeddingsGenerationTasks(ServerStore serverStore) 
        : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActiveAiIntegrationTasks)
    {
    }
    protected override int GetCount(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
    {
        return GetNumberOfActiveEmbeddingsGenerationTasks(rachisState, nodeTag, database);
    }
}
