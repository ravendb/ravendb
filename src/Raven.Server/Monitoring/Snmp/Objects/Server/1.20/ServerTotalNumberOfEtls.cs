using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public sealed class ServerTotalNumberOfEtls : ScalarObjectBase<Integer32>
{
    private readonly ServerStore _store;

    public ServerTotalNumberOfEtls(ServerStore store)
        : base(SnmpOids.Server.TotalNumberOfEtls)
    {
        _store = store;
    }

    protected override Integer32 GetData()
    {
        var result = 0;

        foreach (var database in _store.DatabasesLandlord.GetLoadedDatabases())
            result += database.EtlLoader.GetEtlProcesses().Length;

        return new Integer32(result);
    }
}

