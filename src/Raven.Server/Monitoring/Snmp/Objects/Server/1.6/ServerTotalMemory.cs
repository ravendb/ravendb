using System.Diagnostics;
using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerTotalMemory : ScalarObjectBase<Gauge32>
    {
        public ServerTotalMemory()
            : base(SnmpOids.Server.TotalMemory)
        {
        }

        protected override Gauge32 GetData()
        {
            using (var p = Process.GetCurrentProcess())
                return new Gauge32(p.PrivateMemorySize64 / 1024L / 1024L);
        }
    }
}
