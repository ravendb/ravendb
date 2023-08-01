using Lextm.SharpSnmpLib;
using Raven.Server.Config;
using System.Linq;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerPublicTcpUrl : ScalarObjectBase<OctetString>
    {
        private readonly OctetString _url;

        public ServerPublicTcpUrl(RavenConfiguration configuration)
            : base(SnmpOids.Server.PublicTcpUrl)
        {
            if (configuration.Core.PublicTcpServerUrl.HasValue)
                _url = new OctetString(configuration.Core.PublicTcpServerUrl.Value.UriValue);
            else if (configuration.Core.ExternalPublicTcpServerUrl != null && configuration.Core.ExternalPublicTcpServerUrl.Length > 0)
                _url = new OctetString(string.Join(";", configuration.Core.ExternalPublicTcpServerUrl.Select(x => x.UriValue)));
        }

        protected override OctetString GetData()
        {
            return _url;
        }
    }
}
