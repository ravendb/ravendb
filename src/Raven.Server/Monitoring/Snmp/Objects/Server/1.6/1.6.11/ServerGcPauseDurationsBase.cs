using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public abstract class ServerGcPauseDurationsBase : ServerGcBase<TimeTicks>, ITaggedMetricInstrument<long>
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

        public Measurement<long> GetCurrentMeasurement()
        {
            return new Measurement<long>(GetData()?.ToTimeSpan().Ticks ?? 0, MeasurementTag,
                new KeyValuePair<string, object>("pauseDurationsIndex", _pauseDurationsIndex));
        }
    }
}
