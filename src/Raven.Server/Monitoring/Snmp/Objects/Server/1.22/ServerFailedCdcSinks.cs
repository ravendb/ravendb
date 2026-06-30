using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public sealed class ServerFailedCdcSinks : ScalarObjectBase<Integer32>
{
    private readonly ServerStore _store;

    public ServerFailedCdcSinks(ServerStore store)
        : base(SnmpOids.Server.NumberOfFailedCdcSinks)
    {
        _store = store;
    }

    protected override Integer32 GetData()
    {
        var result = 0;

        foreach (var database in _store.DatabasesLandlord.GetLoadedDatabases())
            result += database.CdcSinkLoader.Processes.Count(x => x.Statistics.HealthStatus == EtlProcessHealthStatus.Failed);

        return new Integer32(result);
    }
}
