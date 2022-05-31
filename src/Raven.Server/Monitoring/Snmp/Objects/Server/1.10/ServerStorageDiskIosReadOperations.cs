using System;
using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class ServerStorageDiskIosReadOperations : ScalarObjectBase<Gauge32>
{
    private readonly ServerStore _store;

    public ServerStorageDiskIosReadOperations(ServerStore store)
        : base(SnmpOids.Server.StorageDiskIoReadOperations)
    {
        _store = store;
    }
        
    protected override Gauge32 GetData()
    {
        if (_store.Configuration.Core.RunInMemory)
            return null;

        var result = _store.Server.DiskStatsGetter.Get(_store._env.Options.DriveInfoByPath?.Value.BasePath.DriveName);
        return result == null ? null : new Gauge32((int)Math.Round(result.IoReadOperations));
    }
}
