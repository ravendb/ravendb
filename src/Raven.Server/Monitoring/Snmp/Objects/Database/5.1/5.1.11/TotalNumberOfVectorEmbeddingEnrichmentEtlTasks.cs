using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfVectorEmbeddingEnrichmentEtlTasks : OngoingTasksBase
{
    public TotalNumberOfVectorEmbeddingEnrichmentEtlTasks(ServerStore serverStore) 
        : base(serverStore, SnmpOids.Databases.General.TotalNumberOfVectorEmbeddingEnrichmentEtlTasks)
    {
    }

    protected override int GetCount(TransactionOperationContext context, RawDatabaseRecord database)
    {
        return GetNumberOfVectorEmbeddingEnrichmentEtls(database);
    }
}
