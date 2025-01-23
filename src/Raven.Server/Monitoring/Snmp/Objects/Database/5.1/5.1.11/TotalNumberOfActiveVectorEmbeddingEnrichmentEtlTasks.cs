using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class TotalNumberOfActiveVectorEmbeddingEnrichmentEtlTasks : ActiveOngoingTasksBase
{
    public TotalNumberOfActiveVectorEmbeddingEnrichmentEtlTasks(ServerStore serverStore) 
        : base(serverStore, SnmpOids.Databases.General.TotalNumberOfActiveVectorEmbeddingEnrichmentEtlTasks)
    {
    }
    protected override int GetCount(TransactionOperationContext context, RachisState rachisState, string nodeTag, RawDatabaseRecord database)
    {
        return GetNumberOfActiveVectorEmbeddingEnrichmentEtls(rachisState, nodeTag, database);
    }
}
