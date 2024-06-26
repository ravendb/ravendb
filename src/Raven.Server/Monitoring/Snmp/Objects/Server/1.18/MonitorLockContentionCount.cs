using System.Threading;
using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class MonitorLockContentionCount : ScalarObjectBase<Integer32>
    {
        public MonitorLockContentionCount()
            : base(SnmpOids.Server.MonitorLockContentionCount)
        {
        }

        protected override Integer32 GetData()
        {
            return new Integer32((int)Monitor.LockContentionCount);
        }
    }
}
