using Raven.Server.Documents.TasksErrors;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseFailedCdcSinks : DatabaseScalarObjectBase<Integer32>
{
    public DatabaseFailedCdcSinks(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.NumberOfFailedCdcSinks, index)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32(database.CdcSinkLoader.Processes.Count(x => x.Statistics.HealthStatus == OngoingTaskHealthStatus.Failed));
    }
}
