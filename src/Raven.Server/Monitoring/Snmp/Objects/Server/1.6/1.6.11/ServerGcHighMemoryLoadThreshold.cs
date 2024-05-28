using System;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerGcHighMemoryLoadThreshold : ServerGcBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        public ServerGcHighMemoryLoadThreshold(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcHighMemoryLoadThreshold)
        {
        }

        private long Value => new Size(GetGCMemoryInfo().HighMemoryLoadThresholdBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public Measurement<long> GetCurrentMeasurement() => new(Value, MeasurementTag);
    }
}
