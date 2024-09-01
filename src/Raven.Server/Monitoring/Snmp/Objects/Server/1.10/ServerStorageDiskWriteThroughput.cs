using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class ServerStorageDiskWriteThroughput(ServerStore store) : ScalarObjectBase<Gauge32>(SnmpOids.Server.StorageDiskWriteThroughput), IMetricInstrument<long>
{
    private long? Value
    {
        get
        {
            if (store.Configuration.Core.RunInMemory)
                return null;
            
            var result = store.Server.DiskStatsGetter.Get(store._env.Options.DriveInfoByPath?.Value.BasePath.DriveName);
            return result?.WriteThroughput.GetValue(SizeUnit.Kilobytes);
        }
    }
        
    protected override Gauge32 GetData()
    {
        var result = Value;
        return result == null ? null : new Gauge32(result.Value);
    }

    public long GetCurrentMeasurement() => Value ?? -1;
}
