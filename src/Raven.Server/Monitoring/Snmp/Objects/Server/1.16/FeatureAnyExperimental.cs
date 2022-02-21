using System.Globalization;
using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class FeatureAnyExperimental : ScalarObjectBase<OctetString>
    {
        private readonly ServerStore _serverStore;

        public FeatureAnyExperimental(ServerStore serverStore)
            : base(SnmpOids.Server.FeatureAnyExperimental)
        {
            _serverStore = serverStore;
        }

        protected override OctetString GetData()
        {
            return new OctetString(_serverStore.FeatureGuardian.AnyExperimental.ToString(CultureInfo.InvariantCulture));
        }
    }
}
