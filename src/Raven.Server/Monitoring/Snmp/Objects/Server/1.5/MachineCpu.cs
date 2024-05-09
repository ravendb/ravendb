using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;
using Raven.Server.Utils.Cpu;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class MachineCpu : ScalarObjectBase<Gauge32>, ITaggedMetricInstrument<int>
    {
        private readonly ICpuUsageCalculator _calculator;
        private readonly KeyValuePair<string, object> _nodeTag;
        private readonly MetricCacher _metricCacher;

        public MachineCpu(MetricCacher metricCacher, ICpuUsageCalculator calculator, KeyValuePair<string, object> nodeTag = default)
            : base(SnmpOids.Server.MachineCpu)
        {
            _metricCacher = metricCacher;
            _calculator = calculator;
            _nodeTag = nodeTag;
        }

        private int Value => (int)_metricCacher.GetValue(MetricCacher.Keys.Server.CpuUsage, _calculator.Calculate).MachineCpuUsage;

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }
        
        public Measurement<int> GetCurrentValue()
        {
            return new(Value, _nodeTag);
        }
    }
}
