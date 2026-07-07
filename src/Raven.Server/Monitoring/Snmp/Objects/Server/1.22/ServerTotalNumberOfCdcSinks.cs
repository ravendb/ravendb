using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public sealed class ServerTotalNumberOfCdcSinks : ScalarObjectBase<Integer32>
{
    private readonly ServerStore _store;

    public ServerTotalNumberOfCdcSinks(ServerStore store)
        : base(SnmpOids.Server.TotalNumberOfCdcSinks)
    {
        _store = store;
    }

    protected override Integer32 GetData()
    {
        var result = 0;

        foreach (var database in _store.DatabasesLandlord.GetLoadedDatabases())
            result += database.CdcSinkLoader.Processes.Length;

        return new Integer32(result);
    }
}
