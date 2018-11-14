using Lextm.SharpSnmpLib;
using Raven.Server.Utils.Cpu;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class MachineCpu : ScalarObjectBase<Gauge32>
    {
        public MachineCpu()
            : base(SnmpOids.Server.MachineCpu)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32((int)CpuUsage.Calculate().MachineCpuUsage);
        }
    }
}
