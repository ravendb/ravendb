using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerBackupsMax : ScalarObjectBase<Integer32>
    {
        private readonly ServerStore _serverStore;

        public ServerBackupsMax(ServerStore serverStore)
            : base(SnmpOids.Server.ServerBackupsMax)
        {
            _serverStore = serverStore;
        }

        protected override Integer32 GetData()
        {
            return new Integer32(_serverStore.ConcurrentBackupsCounter.MaxNumberOfConcurrentBackups);
        }
    }
}
