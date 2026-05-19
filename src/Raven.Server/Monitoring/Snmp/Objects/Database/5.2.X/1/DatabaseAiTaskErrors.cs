using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseAiTaskErrors : DatabaseScalarObjectBase<Integer32>
{
    public DatabaseAiTaskErrors(string databaseName, DatabasesLandlord landlord, int index) : base(databaseName, landlord, SnmpOids.Databases.AiTaskErrors, index)
    {
    }
    
    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32((int)GetCount(database));
    }
    
    private static long GetCount(DocumentDatabase database)
    {
        return database.TaskErrorsStorage.ReadTotalErrorsCount(TaskCategory.Ai);
    }
}
