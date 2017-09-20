using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerFullVersion : ScalarObjectBase<OctetString>
    {
        private readonly OctetString _buildVersion;

        public ServerFullVersion()
            : base("1.2.2")
        {
            _buildVersion = new OctetString(ServerWide.ServerVersion.FullVersion);
        }

        protected override OctetString GetData()
        {
            return _buildVersion;
        }
    }
}
