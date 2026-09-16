using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseCdcSinkLastSuccessfulBatchTime : DatabaseCdcSinkScalarObjectBase<TimeTicks>
{
    public DatabaseCdcSinkLastSuccessfulBatchTime(string databaseName, string cdcSinkName, DatabasesLandlord landlord, int databaseIndex, int cdcSinkIndex)
        : base(databaseName, cdcSinkName, landlord, databaseIndex, cdcSinkIndex, SnmpOids.Databases.CdcSinks.LastSuccessfulBatchTime)
    {
    }

    protected override TimeTicks GetData(DocumentDatabase database)
    {
        var cdcSink = GetCdcSink(database);

        var lastBatchTime = cdcSink?.LastBatchTime;

        if (lastBatchTime.HasValue)
            return SnmpValuesHelper.TimeSpanToTimeTicks(SystemTime.UtcNow - lastBatchTime.Value);

        return DefaultValue;
    }

    private static readonly TimeTicks DefaultValue = new TimeTicks(0);
}
