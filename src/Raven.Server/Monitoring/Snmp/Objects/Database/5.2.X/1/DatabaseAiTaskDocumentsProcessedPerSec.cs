using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseAiTaskDocumentsProcessedPerSec : DatabaseEtlScalarObjectBase<Gauge32>
{
    public DatabaseAiTaskDocumentsProcessedPerSec(string databaseName, string etlName, DatabasesLandlord landlord, int databaseIndex, int etlIndex)
        : base(databaseName, etlName, landlord, databaseIndex, etlIndex, SnmpOids.Databases.AiTasks.DocumentsProcessedPerSec)
    {
    }

    protected override Gauge32 GetData(DocumentDatabase database)
    {
        var etl = GetEtl(database);
        return new Gauge32((int)(etl?.Metrics.BatchSizeMeter.OneMinuteRate ?? 0));
    }
}

