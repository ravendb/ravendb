using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerCpu : ScalarObjectBase<Integer32>
    {
        public ServerCpu()
            : base("1.5")
        {
        }

        protected override Integer32 GetData()
        {
            return null; // TODO
        }
    }
}
