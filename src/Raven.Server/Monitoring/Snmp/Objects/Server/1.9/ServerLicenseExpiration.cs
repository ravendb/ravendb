using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerLicenseExpiration : ScalarObjectBase<OctetString>
    {
        private readonly ServerStore _store;

        public ServerLicenseExpiration(ServerStore store)
            : base("1.9.2")
        {
            _store = store;
        }

        protected override OctetString GetData()
        {
            var status = _store.LicenseManager.GetLicenseStatus();
            var expiration = status.FormattedExpiration;
            if (string.IsNullOrWhiteSpace(expiration))
                return null;

            return new OctetString(expiration);
        }
    }
}
