using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerLicenseType : ScalarObjectBase<OctetString>, IMetricInstrument<short>
    {
        private readonly ServerStore _store;

        public ServerLicenseType(ServerStore store)
            : base(SnmpOids.Server.ServerLicenseType)
        {
            _store = store;
        }

        protected override OctetString GetData()
        {
            var status = _store.LicenseManager.LicenseStatus;
            return new OctetString(status.Type.ToString());
        }

        public short GetCurrentMeasurement()
        {
            return (short)_store.LicenseManager.LicenseStatus.Type;
        }
    }
}
