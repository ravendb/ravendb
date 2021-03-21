using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcMemoryLoad : ServerGcBase<Gauge32>
    {
        public ServerGcMemoryLoad(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcMemoryLoad)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(new Size(GetGCMemoryInfo().MemoryLoadBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes));
        }
    }
}
