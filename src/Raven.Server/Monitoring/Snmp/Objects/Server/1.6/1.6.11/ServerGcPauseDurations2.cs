using System;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcPauseDurations2 : ServerGcPauseDurationsBase
    {
        public ServerGcPauseDurations2(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcPauseDurations2, 1)
        {
        }
    }
}
