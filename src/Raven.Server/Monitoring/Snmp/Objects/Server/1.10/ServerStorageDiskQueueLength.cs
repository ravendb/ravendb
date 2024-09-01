using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class ServerStorageDiskQueueLength(ServerStore store) : ScalarObjectBase<Gauge32>(SnmpOids.Server.StorageDiskQueueLength), IMetricInstrument<long>
{
    private long? Value
    {
        get
        {
            if (store.Configuration.Core.RunInMemory)
                return null;

            var result = store.Server.DiskStatsGetter.Get(store._env.Options.DriveInfoByPath?.Value.BasePath.DriveName);
            
            return result == null || result.QueueLength.HasValue == false 
                ? null 
                : result.QueueLength.Value;
        }
    }

    protected override Gauge32 GetData()
    {
        var result = Value;
        return result.HasValue 
            ? new Gauge32(result.Value) 
            : null;
    }

    public long GetCurrentMeasurement() => Value ?? -1;
}
