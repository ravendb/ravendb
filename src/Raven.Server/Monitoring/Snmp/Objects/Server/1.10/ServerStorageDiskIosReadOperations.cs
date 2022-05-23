using System;
using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class ServerStorageDiskIosReadOperations : ScalarObjectBase<Gauge32>
{
    private readonly ServerStore _store;
    private static readonly Gauge32 Empty = new Gauge32(-1);

    public ServerStorageDiskIosReadOperations(ServerStore store)
        : base(SnmpOids.Server.StorageDiskIoReadOperations)
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
