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
            if (configuration.Core.TcpServerUrls != null && configuration.Core.TcpServerUrls.Length > 0)
                _url = new OctetString(string.Join(";", configuration.Core.TcpServerUrls));
        }

        protected override OctetString GetData()
        {
            return _url;
        }
    }
}
