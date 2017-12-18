using System.Diagnostics;
using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerPid : ScalarObjectBase<Integer32>
    {
        private readonly Integer32 _pid;

        public ServerPid()
            : base(SnmpOids.Server.Pid)
        {
            using (var currentProcess = Process.GetCurrentProcess())
                _pid = new Integer32(currentProcess.Id);
        }

        protected override Integer32 GetData()
        {
            return _pid;
        }
    }
}
