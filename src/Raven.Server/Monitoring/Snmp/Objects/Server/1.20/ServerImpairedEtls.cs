using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public class ServerImpairedEtls : ScalarObjectBase<Integer32>
{
    private readonly ServerStore _store;
    
    public ServerImpairedEtls(ServerStore store)
        : base(SnmpOids.Server.NumberOfImpairedEtls)
    {
        _store = store;
    }

    protected override Integer32 GetData()
    {
        var result = 0;

        foreach (var database in _store.DatabasesLandlord.GetLoadedDatabases())
            result += database.EtlLoader.GetEtlProcesses().Count(x => x.Statistics.HealthStatus == EtlProcessHealthStatus.Impaired);

        return new Integer32(result);
    }
}
