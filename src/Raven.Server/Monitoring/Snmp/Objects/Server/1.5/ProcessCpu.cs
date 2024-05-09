using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;
using Raven.Server.Utils.Cpu;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ProcessCpu : ScalarObjectBase<Gauge32>, ITaggedMetricInstrument<int>
    {
        private readonly KeyValuePair<string, object> _nodeTag;
        private readonly MetricCacher _metricCacher;
        private readonly ICpuUsageCalculator _calculator;
        
        public ProcessCpu(MetricCacher metricCacher, ICpuUsageCalculator calculator, KeyValuePair<string, object> nodeTag = default) : this(metricCacher, calculator)
        {
            _nodeTag = nodeTag;
        }
        
        public ProcessCpu(MetricCacher metricCacher, ICpuUsageCalculator calculator)
            : base(SnmpOids.Server.ProcessCpu)
        {
            _metricCacher = metricCacher;
            _calculator = calculator;
        }
        
        private int GetValue => (int)_metricCacher.GetValue(MetricCacher.Keys.Server.CpuUsage, _calculator.Calculate).ProcessCpuUsage;


        protected override Gauge32 GetData()
        {
            return new Gauge32(GetValue);
        }

        public Measurement<int> GetCurrentValue()
        {
            return new(GetValue, _nodeTag);
        }
    }
}
