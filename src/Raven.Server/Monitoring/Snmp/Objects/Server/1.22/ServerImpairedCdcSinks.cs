using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public sealed class ServerImpairedCdcSinks : ScalarObjectBase<Integer32>
{
    private readonly ServerStore _store;

    public ServerImpairedCdcSinks(ServerStore store)
        : base(SnmpOids.Server.NumberOfImpairedCdcSinks)
    {
        _store = store;
    }

    protected override Integer32 GetData()
    {
        var result = 0;

        foreach (var database in _store.DatabasesLandlord.GetLoadedDatabases())
            result += database.CdcSinkLoader.Processes.Count(x => x.HealthStatus == EtlProcessHealthStatus.Impaired);

        return new Integer32(result);
    }
}
