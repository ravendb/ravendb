using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfEmbeddingsGenerationTasks : OngoingTasksBase
{
    public TotalNumberOfEmbeddingsGenerationTasks(ServerStore serverStore) 
        : base(serverStore, SnmpOids.Databases.General.TotalNumberOfEmbeddingGenerationTasks)
    {
    }

    protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
    {
        return GetNumberOfEmbeddingsGenerationTasks(database);
    }
}
