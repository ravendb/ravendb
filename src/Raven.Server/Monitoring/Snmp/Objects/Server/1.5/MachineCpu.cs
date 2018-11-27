using Lextm.SharpSnmpLib;
using Raven.Server.Utils.Cpu;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class MachineCpu : ScalarObjectBase<Gauge32>
    {
        private readonly ICpuUsageCalculator _calculator;

        public MachineCpu(ICpuUsageCalculator calculator)
            : base(SnmpOids.Server.MachineCpu)
        {
            _calculator = calculator;
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32((int)_calculator.Calculate().MachineCpuUsage);
        }
    }
}
