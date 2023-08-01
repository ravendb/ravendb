using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Server;

public sealed class ServerGcLohSize : ServerGcBase<Gauge32>
{
    public ServerGcLohSize(MetricCacher metricCacher, GCKind gcKind)
        : base(metricCacher, gcKind, SnmpOids.Server.GcLohSize)
    {
    }

    protected override Gauge32 GetData()
    {
        return new Gauge32(new Size(GetGCMemoryInfo().GenerationInfo[3].SizeAfterBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes));
    }
}
