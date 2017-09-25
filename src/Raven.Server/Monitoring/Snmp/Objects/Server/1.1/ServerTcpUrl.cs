using Lextm.SharpSnmpLib;
using Raven.Server.Config;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerTcpUrl : ScalarObjectBase<OctetString>
    {
        private readonly OctetString _url;

        public ServerTcpUrl(RavenConfiguration configuration)
            : base(SnmpOids.Server.TcpUrl)
        {
            if (configuration.Core.TcpServerUrl != null)
                _url = new OctetString(configuration.Core.TcpServerUrl);
        }

        protected override OctetString GetData()
        {
            return _url;
        }
    }
}
