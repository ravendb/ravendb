using System;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerGcGeneration : ServerGcBase<Integer32>, ITaggedMetricInstrument<int>
    {
        public ServerGcGeneration(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcGeneration)
        {
        }

        private int Value => GetGCMemoryInfo().Generation;

        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public Measurement<int> GetCurrentMeasurement() => new(Value, MeasurementTag);
    }
}
