using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseImpairedEtls : DatabaseScalarObjectBase<Integer32>
{
    public DatabaseImpairedEtls(string databaseName, DatabasesLandlord landlord, int index) : base(databaseName, landlord, SnmpOids.Databases.NumberOfImpairedEtls, index)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32(GetCount(database));
    }
    
    private static int GetCount(DocumentDatabase database)
    {
        return database.EtlLoader.Processes.Count(x => x.Statistics.HealthStatus == EtlProcessHealthStatus.Impaired);
    }
}
