using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class WellKnownAdminCertificates : ScalarObjectBase<OctetString>
    {
        private readonly OctetString _wellKnownAdminCertificates;

        public WellKnownAdminCertificates(ServerStore store)
            : base(SnmpOids.Server.WellKnownAdminCertificates)
        {
            var wellKnownAdminCertificates = store.Configuration.Security.WellKnownAdminCertificates;
            if (wellKnownAdminCertificates == null)
                return;

            _wellKnownAdminCertificates = new OctetString(string.Join(";", wellKnownAdminCertificates));
        }

        protected override OctetString GetData()
        {
            return _wellKnownAdminCertificates;
        }
    }
}
