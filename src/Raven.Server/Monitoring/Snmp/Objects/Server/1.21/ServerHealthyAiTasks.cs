using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public sealed class ServerHealthyAiTasks : ScalarObjectBase<Integer32>
{
    private readonly ServerStore _store;

    public ServerHealthyAiTasks(ServerStore store)
        : base(SnmpOids.Server.NumberOfHealthyAiTasks)
    {
        _store = store;
    }

    protected override Integer32 GetData()
    {
        var result = 0;

        foreach (var database in _store.DatabasesLandlord.GetLoadedDatabases())
        {
            result += database.EtlLoader.GetAiProcesses()
                .Count(x => x.Statistics.HealthStatus == EtlProcessHealthStatus.Healthy);
        }

        return new Integer32(result);
    }
}

