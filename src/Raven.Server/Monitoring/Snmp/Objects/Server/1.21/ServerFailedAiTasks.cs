using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public sealed class ServerFailedAiTasks : ScalarObjectBase<Integer32>
{
    private readonly ServerStore _store;

    public ServerFailedAiTasks(ServerStore store)
        : base(SnmpOids.Server.NumberOfFailedAiTasks)
    {
        _store = store;
    }

    protected override Integer32 GetData()
    {
        var result = 0;

        foreach (var database in _store.DatabasesLandlord.GetLoadedDatabases())
        {
            result += database.EtlLoader.GetAiProcesses()
                .Count(x => x.Statistics.HealthStatus == EtlProcessHealthStatus.Failed);
        }

        return new Integer32(result);
    }
}

