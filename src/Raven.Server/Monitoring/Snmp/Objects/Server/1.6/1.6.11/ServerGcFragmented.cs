using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcPromoted : ServerGcBase<Gauge32>
    {
        public ServerGcPromoted(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcPromoted)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(new Size(GetGCMemoryInfo().PromotedBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes));
        }
    }
}
