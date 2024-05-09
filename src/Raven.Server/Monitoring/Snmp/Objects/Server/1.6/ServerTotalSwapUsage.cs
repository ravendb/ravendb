using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.LowMemory;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerTotalSwapUsage : ScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        private readonly MetricCacher _metricCacher;
        private readonly KeyValuePair<string, object> _nodeTag;

        public ServerTotalSwapUsage(MetricCacher metricCacher, KeyValuePair<string, object> nodeTag = default)
            : base(SnmpOids.Server.TotalSwapUsage)
        {
            _metricCacher = metricCacher;
            _nodeTag = nodeTag;
        }
        
        private long Value => _metricCacher.GetValue<MemoryInfoResult>(
            MetricCacher.Keys.Server.MemoryInfoExtended.RefreshRate15Seconds).TotalSwapUsage.GetValue(SizeUnit.Megabytes);

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public Measurement<long> GetCurrentValue()
        {
            return new(Value, _nodeTag);
        }
    }
}
