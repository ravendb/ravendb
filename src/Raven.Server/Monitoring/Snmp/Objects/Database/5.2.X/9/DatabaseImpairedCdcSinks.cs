using Raven.Server.Documents.TasksErrors;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseImpairedCdcSinks : DatabaseScalarObjectBase<Integer32>
{
    public DatabaseImpairedCdcSinks(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.NumberOfImpairedCdcSinks, index)
    {
    }

    protected override Integer32 GetData(DocumentDatabase database)
    {
        return new Integer32(database.CdcSinkLoader.Processes.Count(x => x.Statistics.HealthStatus == OngoingTaskHealthStatus.Impaired));
    }
}
