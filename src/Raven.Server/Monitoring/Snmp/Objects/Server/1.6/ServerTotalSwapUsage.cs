using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerTotalSwapUsage : ScalarObjectBase<Gauge32>, IMetricInstrument<long>
    {
        private readonly MetricCacher _metricCacher;

        public ServerTotalSwapUsage(MetricCacher metricCacher)
            : base(SnmpOids.Server.TotalSwapUsage)
        {
            _metricCacher = metricCacher;
        }
        
        private long Value => _metricCacher.GetValue<MemoryInfoResult>(
            MetricCacher.Keys.Server.MemoryInfoExtended.RefreshRate15Seconds).TotalSwapUsage.GetValue(SizeUnit.Megabytes);

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public long GetCurrentMeasurement() => Value;
    }
}
