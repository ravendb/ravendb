using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseErrorsOfAiTask : DatabaseEtlScalarObjectBase<Integer32>
{
    public DatabaseErrorsOfAiTask(string databaseName, string etlName, DatabasesLandlord landlord, int databaseIndex, int aiTaskIndex)
        : base(databaseName, etlName, landlord, databaseIndex, aiTaskIndex, SnmpOids.Databases.AiTasks.AiTasksErrors)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32((int)database.TaskErrorsStorage.ReadErrorsCountOfTask(TaskCategory.Ai, EtlName));
    }
}

