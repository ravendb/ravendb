using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseEtlErrors : DatabaseScalarObjectBase<Integer32>
{
    public DatabaseEtlErrors(string databaseName, DatabasesLandlord landlord, int index) : base(databaseName, landlord, SnmpOids.Databases.EtlErrors, index)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32((int)GetCount(database));
    }
    
    private static long GetCount(DocumentDatabase database)
    {
        return database.TaskErrorsStorage.ReadTotalErrorsCount(TaskCategory.Etl);
    }
}
