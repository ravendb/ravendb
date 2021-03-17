using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcHighMemoryLoadThreshold : ServerGcBase<Gauge32>
    {
        public ServerGcHighMemoryLoadThreshold(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcHighMemoryLoadThreshold)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(new Size(GetGCMemoryInfo().HighMemoryLoadThresholdBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes));
        }
    }
}
