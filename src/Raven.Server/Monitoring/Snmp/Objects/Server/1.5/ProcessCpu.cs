using Lextm.SharpSnmpLib;
using Raven.Server.Utils.Cpu;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ProcessCpu : ScalarObjectBase<Gauge32>
    {
        private readonly ICpuUsageCalculator _calculator;

        public ProcessCpu(ICpuUsageCalculator calculator)
            : base(SnmpOids.Server.ProcessCpu)
        {
            _calculator = calculator;
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32((int)_calculator.Calculate().ProcessCpuUsage);
        }
    }
}
