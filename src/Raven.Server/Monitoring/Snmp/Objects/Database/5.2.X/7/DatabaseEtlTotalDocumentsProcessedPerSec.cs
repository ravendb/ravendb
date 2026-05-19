using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseEtlTotalDocumentsProcessedPerSec : DatabaseScalarObjectBase<Gauge32>
{
    public DatabaseEtlTotalDocumentsProcessedPerSec(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.EtlDocumentsProcessedPerSec, index)
    {
    }

    protected override Gauge32 GetData(DocumentDatabase database)
    {
        var rate = 0.0;
        foreach (EtlProcess etl in database.EtlLoader.GetEtlProcesses())
            rate += etl.Metrics.BatchSizeMeter.OneMinuteRate;
        return new Gauge32((int)rate);
    }
}
