using Lextm.SharpSnmpLib;
using Raven.Server.Documents.ETL;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public class ServerAiTasksErrors : ScalarObjectBase<Integer32>
{
    private readonly ServerStore _store;
    
    public ServerAiTasksErrors(ServerStore store)
        : base(SnmpOids.Server.AiTasksErrors)
    {
        _store = store;
    }

    protected override Integer32 GetData()
    {
        var result = 0;

        foreach (var database in _store.DatabasesLandlord.GetLoadedDatabases())
            result += (int)database.TaskErrorsStorage.ReadTotalErrorsCount(TaskCategory.Ai);

        return new Integer32(result);
    }
}
