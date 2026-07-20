using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;
using Raven.Server.Documents.TasksErrors;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public sealed class ServerCdcSinkErrors : ScalarObjectBase<Integer32>
{
    private readonly ServerStore _store;

    public ServerCdcSinkErrors(ServerStore store)
        : base(SnmpOids.Server.CdcSinkErrors)
    {
        _store = store;
    }

    protected override Integer32 GetData()
    {
        var result = 0;

        foreach (var database in _store.DatabasesLandlord.GetLoadedDatabases())
            result += (int)database.TaskErrorsStorage.ReadTotalErrorsCount(TaskCategory.CdcSink);

        return new Integer32(result);
    }
}
