using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseAiTaskTotalDocumentsProcessedPerSec : DatabaseScalarObjectBase<Gauge32>
{
    public DatabaseAiTaskTotalDocumentsProcessedPerSec(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.AiTaskDocumentsProcessedPerSec, index)
    {
    }

    protected override Gauge32 GetData(DocumentDatabase database)
    {
        var rate = 0.0;
        foreach (EtlProcess etl in database.EtlLoader.GetAiProcesses())
            rate += etl.Metrics.BatchSizeMeter.OneMinuteRate;
        return new Gauge32((int)rate);
    }
}
