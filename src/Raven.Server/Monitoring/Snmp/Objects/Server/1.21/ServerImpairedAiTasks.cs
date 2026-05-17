using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public sealed class ServerImpairedAiTasks : ScalarObjectBase<Integer32>
{
    private readonly ServerStore _store;

    public ServerImpairedAiTasks(ServerStore store)
        : base(SnmpOids.Server.NumberOfImpairedAiTasks)
    {
        _store = store;
    }

    protected override Integer32 GetData()
    {
        var result = 0;

        foreach (var database in _store.DatabasesLandlord.GetLoadedDatabases())
        {
            result += database.EtlLoader.GetAiProcesses()
                .Count(x => x.Statistics.HealthStatus == EtlProcessHealthStatus.Impaired);
        }

        return new Integer32(result);
    }
}

