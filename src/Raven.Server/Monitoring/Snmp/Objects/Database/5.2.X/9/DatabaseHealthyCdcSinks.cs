using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseHealthyCdcSinks : DatabaseScalarObjectBase<Integer32>
{
    public DatabaseHealthyCdcSinks(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.NumberOfHealthyCdcSinks, index)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32(database.CdcSinkLoader.Processes.Count(x => x.Statistics.HealthStatus == EtlProcessHealthStatus.Healthy));
    }
}
