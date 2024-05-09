using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class ServerStorageDiskReadThroughput : ScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
{
    private readonly ServerStore _store;
    private readonly KeyValuePair<string, object> _nodeTag;

    public ServerStorageDiskReadThroughput(ServerStore store, KeyValuePair<string, object> nodeTag = default)
        : base(SnmpOids.Server.StorageDiskReadThroughput)
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
            return result?.ReadThroughput.GetValue(SizeUnit.Kilobytes);
        }
    }

    protected override Gauge32 GetData()
    {
        var result = Value;
        return result == null ? null : new Gauge32(result.Value);
    }

    public Measurement<long> GetCurrentValue()
    {
        return new(Value ?? -1, _nodeTag);
    }
}
