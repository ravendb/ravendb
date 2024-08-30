using System;
using System.Diagnostics.Metrics;
using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerGcCompacted : ServerGcBase<OctetString>, ITaggedMetricInstrument<byte>
    {
        public ServerGcCompacted(MetricCacher metricCacher, GCKind gcKind)
            : base(metricCacher, gcKind, SnmpOids.Server.GcCompacted)
        {
        }

        private bool Value => GetGCMemoryInfo().Compacted;

        public override ISnmpData Data => new OctetString(Value.ToString(CultureInfo.InvariantCulture));

        protected override OctetString GetData()
        {
            throw new NotSupportedException();
        }

        public Measurement<byte> GetCurrentMeasurement() => new((byte)(Value ? 1 : 0), MeasurementTag);
    }
}
