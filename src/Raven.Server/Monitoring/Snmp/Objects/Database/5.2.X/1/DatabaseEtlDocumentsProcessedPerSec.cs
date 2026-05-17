using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseEtlDocumentsProcessedPerSec : DatabaseEtlScalarObjectBase<Gauge32>
{
    public DatabaseEtlDocumentsProcessedPerSec(string databaseName, string etlName, DatabasesLandlord landlord, int databaseIndex, int etlIndex)
        : base(databaseName, etlName, landlord, databaseIndex, etlIndex, SnmpOids.Databases.Etls.DocumentsProcessedPerSec)
    {
    }

    protected override Gauge32 GetData(DocumentDatabase database)
    {
        var etl = GetEtl(database);
        return new Gauge32((int)(etl?.Metrics.BatchSizeMeter.OneMinuteRate ?? 0));
    }
}

