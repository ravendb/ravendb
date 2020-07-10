using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerLicenseMaxCpuCores : ScalarObjectBase<Integer32>
    {
        private readonly ServerStore _store;

        public ServerLicenseMaxCpuCores(ServerStore store)
            : base(SnmpOids.Server.ServerLicenseMaxCpuCores)
        {
            _store = store;
        }

        protected override Integer32 GetData()
        {
            var status = _store.LicenseManager.LicenseStatus;
            return new Integer32(status.MaxCores);
        }
    }
}
