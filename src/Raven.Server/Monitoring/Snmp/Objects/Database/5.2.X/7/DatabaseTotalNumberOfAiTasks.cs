using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseTotalNumberOfAiTasks : DatabaseScalarObjectBase<Integer32>
{
    public DatabaseTotalNumberOfAiTasks(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.TotalNumberOfAiTasks, index)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32(database.EtlLoader.GetAiProcesses().Length);
    }
}

