using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerWorkingSetSwapUsage : ScalarObjectBase<Gauge32>, IMetricInstrument<long>
    {
        private readonly MetricCacher _metricCacher;


        public ServerWorkingSetSwapUsage(MetricCacher metricCacher)
            : base(SnmpOids.Server.WorkingSetSwapUsage)
        {
            _metricCacher = metricCacher;
        }

        private long Value => _metricCacher.GetValue<MemoryInfoResult>(
            MetricCacher.Keys.Server.MemoryInfoExtended.RefreshRate15Seconds).WorkingSetSwapUsage.GetValue(SizeUnit.Megabytes);
        
        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public long GetCurrentMeasurement() => Value;
    }
}
