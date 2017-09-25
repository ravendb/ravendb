using Lextm.SharpSnmpLib;
using Raven.Server.Config;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerPublicUrl : ScalarObjectBase<OctetString>
    {
        private readonly OctetString _url;

        public ServerPublicUrl(RavenConfiguration configuration)
            : base(SnmpOids.Server.PublicUrl)
        {
            if (configuration.Core.PublicServerUrl.HasValue)
                _url = new OctetString(configuration.Core.PublicServerUrl.Value.UriValue);
        }

        protected override OctetString GetData()
        {
            return _url;
        }
    }
}
