using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseTotalNumberOfEtls : DatabaseScalarObjectBase<Integer32>
{
    public DatabaseTotalNumberOfEtls(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.TotalNumberOfEtls, index)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32(database.EtlLoader.GetEtlProcesses().Length);
    }
}

