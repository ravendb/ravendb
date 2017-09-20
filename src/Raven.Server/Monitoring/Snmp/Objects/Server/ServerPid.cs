using System.Diagnostics;
using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerPid : ScalarObjectBase<Integer32>
    {
        private readonly Integer32 _pid;

        public ServerPid()
            : base("1.4")
        {
            _pid = new Integer32(Process.GetCurrentProcess().Id);
        }

        protected override Integer32 GetData()
        {
            return _pid;
        }
    }
}
