using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public abstract class ServerGcPauseDurationsBase : ServerGcBase<TimeTicks>
    {
        private readonly int _pauseDurationsIndex;

        protected ServerGcPauseDurationsBase(MetricCacher metricCacher, GCKind gcKind, string dots, int pauseDurationsIndex)
            : base(metricCacher, gcKind, dots)
        {
            _pauseDurationsIndex = pauseDurationsIndex;
        }

        protected override TimeTicks GetData()
        {
            var memoryInfo = GetGCMemoryInfo();
            if (memoryInfo.PauseDurations.IsEmpty)
                return null;

            if (memoryInfo.PauseDurations.Length > _pauseDurationsIndex)
                return new TimeTicks(memoryInfo.PauseDurations[_pauseDurationsIndex]);

            return null;
        }
    }
}
