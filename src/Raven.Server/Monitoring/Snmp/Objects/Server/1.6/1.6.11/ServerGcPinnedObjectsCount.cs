using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcPinnedObjectsCount : ServerGcBase<Gauge32>
    {
        public ServerGcPinnedObjectsCount(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcPinnedObjectsCount)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(GetGCMemoryInfo().PinnedObjectsCount);
        }
    }
}
