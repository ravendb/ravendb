using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerVersion : ScalarObjectBase<OctetString>
    {
        private readonly OctetString _productVersion;

        public ServerVersion()
            : base("1.2.1")
        {
            _productVersion = new OctetString(ServerWide.ServerVersion.Version);
        }

        protected override OctetString GetData()
        {
            return _productVersion;
        }
    }
}
