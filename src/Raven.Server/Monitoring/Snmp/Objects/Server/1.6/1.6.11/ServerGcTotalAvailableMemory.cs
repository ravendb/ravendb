using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcTotalCommitted : ServerGcBase<Gauge32>
    {
        public ServerGcTotalCommitted(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcTotalCommitted)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(new Size(GetGCMemoryInfo().TotalCommittedBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes));
        }
    }
}
