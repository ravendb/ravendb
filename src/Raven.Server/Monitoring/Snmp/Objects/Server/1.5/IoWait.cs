using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.Utils;
using Raven.Server.Utils.Cpu;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class IoWait : ScalarObjectBase<Gauge32>, IMetricInstrument<int>
    {
        private readonly MetricCacher _metricCacher;
        private readonly ICpuUsageCalculator _calculator;

        public IoWait(MetricCacher metricCacher, ICpuUsageCalculator calculator)
            : base(SnmpOids.Server.MachineIoWait)
        {
            _metricCacher = metricCacher;
            _calculator = calculator;
        }

        private int Value => (int)_metricCacher.GetValue(MetricCacher.Keys.Server.CpuUsage, _calculator.Calculate).MachineIoWait;

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}
