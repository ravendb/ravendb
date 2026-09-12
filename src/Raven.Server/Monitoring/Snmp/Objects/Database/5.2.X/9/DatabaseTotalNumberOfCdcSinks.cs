using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseTotalNumberOfCdcSinks : DatabaseScalarObjectBase<Integer32>
{
    public DatabaseTotalNumberOfCdcSinks(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.TotalNumberOfCdcSinks, index)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32(database.CdcSinkLoader.Processes.Length);
    }
}
