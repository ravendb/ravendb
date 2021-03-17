using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcHeapSize : ServerGcBase<Gauge32>
    {
        public ServerGcHeapSize(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcHeapSize)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(new Size(GetGCMemoryInfo().HeapSizeBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes));
        }
    }
}
