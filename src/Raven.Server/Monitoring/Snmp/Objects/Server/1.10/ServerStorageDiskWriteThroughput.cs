using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class ServerStorageDiskWriteThroughput : ScalarObjectBase<Gauge32>
{
    private readonly ServerStore _store;
    private static readonly Gauge32 Empty = new Gauge32(-1);

    public ServerStorageDiskWriteThroughput(ServerStore store)
        : base(SnmpOids.Server.StorageDiskWriteThroughput)
    {
        _store = store;
    }
        
    protected override Gauge32 GetData()
    {
        if (_store.Configuration.Core.RunInMemory)
            return Empty;
            
        var result = _store.Server.DiskStatsGetter.Get(_store._env.Options.DriveInfoByPath?.Value.BasePath.DriveName);
        return result == null ? Empty : new Gauge32(result.WriteThroughput.GetValue(SizeUnit.Kilobytes));
    }
}
