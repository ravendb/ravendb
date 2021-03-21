using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerGcGeneration : ServerGcBase<Integer32>
    {
        public ServerGcGeneration(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcGeneration)
        {
        }

        protected override Integer32 GetData()
        {
            return new Integer32(GetGCMemoryInfo().Generation);
        }
    }
}
