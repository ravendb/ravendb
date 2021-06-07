using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcIndex : ServerGcBase<Integer32>
    {
        public ServerGcIndex(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcIndex)
        {
        }

        protected override Integer32 GetData()
        {
            return new Integer32((int)GetGCMemoryInfo().Index);
        }
    }
}
