using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseFailedAiTasks : DatabaseScalarObjectBase<Integer32>
{
    public DatabaseFailedAiTasks(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.NumberOfFailedAiTasks, index)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32(database.EtlLoader.Processes
            .Count(x => x.EtlType is EtlType.EmbeddingsGeneration or EtlType.GenAi && x.Statistics.HealthStatus == EtlProcessHealthStatus.Failed));
    }
}

