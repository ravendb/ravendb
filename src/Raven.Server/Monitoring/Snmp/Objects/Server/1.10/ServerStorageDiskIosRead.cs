using System;
using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class ServerStorageDiskIosRead : ScalarObjectBase<Gauge32>
{
    private readonly ServerStore _store;
    private static readonly Gauge32 Empty = new Gauge32(-1);

    public ServerStorageDiskIosRead(ServerStore store)
        : base(SnmpOids.Server.StorageDiskIoRead)
    {
        _store = store;
    }

        
    protected override Gauge32 GetData()
    {
        if (_store.Configuration.Core.RunInMemory)
            return Empty;

            
        var result = _store.Server.DiskStatsGetter.Get(_store._env.Options.DriveInfoByPath?.Value.BasePath.DriveName);
        return result == null ? Empty : new Gauge32((int)Math.Round(result.IoReadOperations));
    }
}
