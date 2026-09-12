using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseCdcSinkHealthStatus : DatabaseCdcSinkScalarObjectBase<OctetString>
{
    public DatabaseCdcSinkHealthStatus(string databaseName, string cdcSinkName, DatabasesLandlord landlord, int databaseIndex, int cdcSinkIndex)
        : base(databaseName, cdcSinkName, landlord, databaseIndex, cdcSinkIndex, SnmpOids.Databases.CdcSinks.HealthStatus)
    {
    }

    protected override OctetString GetData(DocumentDatabase database)
    {
        var cdcSink = GetCdcSink(database);
        if (cdcSink == null)
            return DefaultValue;

        return new OctetString(cdcSink.Statistics.HealthStatus.ToString());
    }

    private static readonly OctetString DefaultValue = new OctetString("N/A");
}
