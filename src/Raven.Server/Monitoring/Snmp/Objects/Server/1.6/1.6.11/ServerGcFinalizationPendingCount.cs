using System;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerGcFinalizationPendingCount : ServerGcBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        public ServerGcFinalizationPendingCount(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcFinalizationPendingCount)
        {
        }

        private long Value => GetGCMemoryInfo().FinalizationPendingCount;
        
        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public Measurement<long> GetCurrentMeasurement() => new(Value, MeasurementTag);
    }
}
