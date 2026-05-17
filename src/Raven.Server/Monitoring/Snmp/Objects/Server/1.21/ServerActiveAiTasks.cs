using System;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public sealed class ServerActiveAiTasks : ScalarObjectBase<Integer32>
{
    private readonly ServerStore _store;

    public ServerActiveAiTasks(ServerStore store)
        : base(SnmpOids.Server.NumberOfActiveAiTasks)
    {
        _store = store;
    }

    protected override Integer32 GetData()
    {
        var result = 0;
        var oneMinuteAgo = DateTime.UtcNow.AddMinutes(-1);

        foreach (var database in _store.DatabasesLandlord.GetLoadedDatabases())
        {
            result += database.EtlLoader.GetAiProcesses()
                .Count(x => x.GetLatestPerformanceStats()?.StartTime > oneMinuteAgo);
        }

        return new Integer32(result);
    }
}

