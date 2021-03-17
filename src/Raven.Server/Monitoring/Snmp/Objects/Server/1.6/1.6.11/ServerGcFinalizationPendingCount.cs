using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcFinalizationPendingCount : ServerGcBase<Gauge32>
    {
        public ServerGcFinalizationPendingCount(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcFinalizationPendingCount)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(GetGCMemoryInfo().FinalizationPendingCount);
        }
    }
}
