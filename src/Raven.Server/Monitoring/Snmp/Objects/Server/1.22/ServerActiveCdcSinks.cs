using System;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public sealed class ServerActiveCdcSinks : ScalarObjectBase<Integer32>
{
    private readonly ServerStore _store;

    public ServerActiveCdcSinks(ServerStore store)
        : base(SnmpOids.Server.NumberOfActiveCdcSinks)
    {
        _store = store;
    }

    protected override Integer32 GetData()
    {
        var result = 0;
        var oneMinuteAgo = DateTime.UtcNow.AddMinutes(-1);

        foreach (var database in _store.DatabasesLandlord.GetLoadedDatabases())
            result += database.CdcSinkLoader.Processes.Count(x => x.GetLatestPerformanceStats()?.StartTime > oneMinuteAgo);

        return new Integer32(result);
    }
}
