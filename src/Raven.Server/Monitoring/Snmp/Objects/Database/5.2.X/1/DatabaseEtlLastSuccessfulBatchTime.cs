using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class DatabaseEtlLastSuccessfulBatchTime : DatabaseEtlScalarObjectBase<TimeTicks>
{
    public DatabaseEtlLastSuccessfulBatchTime(string databaseName, string etlName, DatabasesLandlord landlord, int databaseIndex, int etlIndex)
        : base(databaseName, etlName, landlord, databaseIndex, etlIndex, SnmpOids.Databases.Etls.LastSuccessfulBatchTime)
    {
    }

    protected override TimeTicks GetData(DocumentDatabase database)
    {
        var etl = GetEtl(database);

        var lastSuccessfulBatchTime = etl?.Statistics.LastSuccessfulBatchTime;

        if (lastSuccessfulBatchTime.HasValue)
            return SnmpValuesHelper.TimeSpanToTimeTicks(SystemTime.UtcNow - lastSuccessfulBatchTime.Value);

        return DefaultValue;
    }

    private static readonly TimeTicks DefaultValue = new TimeTicks(0);
}
