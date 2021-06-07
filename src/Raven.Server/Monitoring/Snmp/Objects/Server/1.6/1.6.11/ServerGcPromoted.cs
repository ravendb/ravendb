using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcFragmented : ServerGcBase<Gauge32>
    {
        public ServerGcFragmented(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcFragmented)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(new Size(GetGCMemoryInfo().FragmentedBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes));
        }
    }
}
