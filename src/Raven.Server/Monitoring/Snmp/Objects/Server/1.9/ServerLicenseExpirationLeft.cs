using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerLicenseExpirationLeft : ScalarObjectBase<TimeTicks>
    {
        private readonly ServerStore _store;

        public ServerLicenseExpirationLeft(ServerStore store)
            : base("1.9.3")
        {
            _store = store;
        }

        protected override TimeTicks GetData()
        {
            var status = _store.LicenseManager.GetLicenseStatus();
            if (status.Expiration.HasValue == false)
                return null;

            return new TimeTicks(SystemTime.UtcNow - status.Expiration.Value);
        }
    }
}
