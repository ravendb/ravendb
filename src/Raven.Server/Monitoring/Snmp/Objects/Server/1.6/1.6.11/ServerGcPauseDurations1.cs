using System;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcPauseDurations1 : ServerGcPauseDurationsBase
    {
        public ServerGcPauseDurations1(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcPauseDurations1, 0)
        {
        }
    }
}
