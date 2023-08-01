using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Raven.Server.Utils.Cpu;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class MachineCpu : ScalarObjectBase<Gauge32>
    {
        private readonly ICpuUsageCalculator _calculator;
        private readonly MetricCacher _metricCacher;

        public MachineCpu(MetricCacher metricCacher, ICpuUsageCalculator calculator)
            : base(SnmpOids.Server.MachineCpu)
        {
            _metricCacher = metricCacher;
            _calculator = calculator;
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32((int)_metricCacher.GetValue(MetricCacher.Keys.Server.CpuUsage, _calculator.Calculate).MachineCpuUsage);
        }
    }
}
