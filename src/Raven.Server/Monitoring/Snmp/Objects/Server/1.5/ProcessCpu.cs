using Lextm.SharpSnmpLib;
using Raven.Server.Utils.Cpu;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ProcessCpu : ScalarObjectBase<Gauge32>
    {
        public ProcessCpu()
            : base(SnmpOids.Server.ProcessCpu)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32((int)CpuUsage.Calculate().ProcessCpuUsage);
        }
    }
}
