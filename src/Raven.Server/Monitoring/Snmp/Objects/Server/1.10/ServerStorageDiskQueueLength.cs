using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class ServerStorageDiskQueueLength : ScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
{
    private readonly ServerStore _store;
    private readonly KeyValuePair<string, object> _nodeTag;

    public ServerStorageDiskQueueLength(ServerStore store, KeyValuePair<string, object> nodeTag = default)
        : base(SnmpOids.Server.StorageDiskQueueLength)
    {
        _store = store;
        _nodeTag = nodeTag;
    }

    private long? Value
    {
        get
        {
            if (_store.Configuration.Core.RunInMemory)
                return null;

            var result = _store.Server.DiskStatsGetter.Get(_store._env.Options.DriveInfoByPath?.Value.BasePath.DriveName);
            
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

    public Measurement<long> GetCurrentValue()
    {
        return new(Value ?? -1, _nodeTag);
    }
}
