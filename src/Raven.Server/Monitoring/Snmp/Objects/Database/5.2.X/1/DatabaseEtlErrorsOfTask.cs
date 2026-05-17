using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;


public sealed class DatabaseEtlErrorsOfTask : DatabaseEtlScalarObjectBase<Integer32>
{
    public DatabaseEtlErrorsOfTask(string databaseName, string etlName, DatabasesLandlord landlord, int databaseIndex, int etlIndex)
        : base(databaseName, etlName, landlord, databaseIndex, etlIndex, SnmpOids.Databases.Etls.EtlErrorsOfTask)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32((int)database.TaskErrorsStorage.ReadErrorsCountOfTask(TaskCategory.Etl, EtlName));
    }
}
