using System;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerGcFragmented : ServerGcBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        public ServerGcFragmented(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcFragmented)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        private long Value
        {
            get
            {
                return new Size(GetGCMemoryInfo().FragmentedBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);
            }
        }

        public Measurement<long> GetCurrentMeasurement() => new(Value, MeasurementTag);
    }
}
