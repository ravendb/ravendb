using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerCertificateExpiration : ScalarObjectBase<OctetString>
    {
        private readonly ServerStore _store;

        public ServerCertificateExpiration(ServerStore store)
            : base(SnmpOids.Server.ServerCertificateExpiration)
        {
            _store = store;
        }

        protected override OctetString GetData()
        {
            var holder = _store.Server.Certificate;
            if (holder == null || holder.Certificate == null)
                return null;

            var notAfter = holder.Certificate.NotAfter.ToUniversalTime();

            return new OctetString(notAfter.ToString("d", CultureInfo.CurrentCulture));
        }
    }
}
