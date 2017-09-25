using Lextm.SharpSnmpLib;
using Raven.Server.Config;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerPublicTcpUrl : ScalarObjectBase<OctetString>
    {
        private readonly OctetString _url;

        public ServerPublicTcpUrl(RavenConfiguration configuration)
            : base(SnmpOids.Server.PublicTcpUrl)
        {
            if (configuration.Core.PublicTcpServerUrl.HasValue)
                _url = new OctetString(configuration.Core.PublicTcpServerUrl.Value.UriValue);
        }

        protected override OctetString GetData()
        {
            return _url;
        }
    }
}
