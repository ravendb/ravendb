using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerLicenseExpiration : ScalarObjectBase<OctetString>
    {
        private readonly ServerStore _store;

        public ServerLicenseExpiration(ServerStore store)
            : base(SnmpOids.Server.ServerLicenseExpiration)
        {
            _store = store;
        }

        protected override OctetString GetData()
        {
            var status = _store.LicenseManager.LicenseStatus;
            if (status.Expiration.HasValue == false)
                return null;

            return new OctetString(status.Expiration.Value.ToString("d", CultureInfo.CurrentCulture));
        }
    }
}
