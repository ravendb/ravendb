using Lextm.SharpSnmpLib;
using Raven.Server.Utils;
using Raven.Server.Utils.Cpu;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class IoWait : ScalarObjectBase<Gauge32>
    {
        private readonly MetricCacher _metricCacher;
        private readonly ICpuUsageCalculator _calculator;

        public IoWait(MetricCacher metricCacher, ICpuUsageCalculator calculator)
            : base(SnmpOids.Server.MachineIoWait)
        {
            _metricCacher = metricCacher;
            _calculator = calculator;
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32((int)_metricCacher.GetValue(MetricCacher.Keys.Server.IoWait, _calculator.Calculate).MachineIoWait);
        }
    }
}
