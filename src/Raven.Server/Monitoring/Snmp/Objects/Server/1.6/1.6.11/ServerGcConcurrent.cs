using System;
using System.Diagnostics.Metrics;
using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerGcConcurrent : ServerGcBase<OctetString>, ITaggedMetricInstrument<byte>
    {
        public ServerGcConcurrent(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcConcurrent)
        {
        }

        private bool Value => GetGCMemoryInfo().Concurrent;
        
        public override ISnmpData Data => new OctetString(Value.ToString(CultureInfo.InvariantCulture));

        protected override OctetString GetData()
        {
            throw new NotSupportedException();
        }

        public Measurement<byte> GetCurrentMeasurement() => new((byte)(Value ? 1 : 0), MeasurementTag);
    }
}
