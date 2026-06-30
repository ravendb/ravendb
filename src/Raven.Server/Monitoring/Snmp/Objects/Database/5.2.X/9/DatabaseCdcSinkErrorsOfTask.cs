using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseCdcSinkErrorsOfTask : DatabaseCdcSinkScalarObjectBase<Integer32>
{
    public DatabaseCdcSinkErrorsOfTask(string databaseName, string cdcSinkName, DatabasesLandlord landlord, int databaseIndex, int cdcSinkIndex)
        : base(databaseName, cdcSinkName, landlord, databaseIndex, cdcSinkIndex, SnmpOids.Databases.CdcSinks.CdcSinkErrorsOfTask)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32((int)database.TaskErrorsStorage.ReadErrorsCountOfTask(TaskCategory.CdcSink, CdcSinkName));
    }
}
