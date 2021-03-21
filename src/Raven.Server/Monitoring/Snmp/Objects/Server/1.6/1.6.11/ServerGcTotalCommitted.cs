using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcTotalAvailableMemory : ServerGcBase<Gauge32>
    {
        public ServerGcTotalAvailableMemory(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcTotalAvailableMemory)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(new Size(GetGCMemoryInfo().TotalAvailableMemoryBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes));
        }
    }
}
