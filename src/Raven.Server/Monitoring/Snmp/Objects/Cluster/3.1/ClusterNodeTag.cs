using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ClusterNodeTag : ScalarObjectBase<OctetString>
    {
        private readonly ServerStore _store;

        public ClusterNodeTag(ServerStore store)
            : base("3.1.1")
        {
            _store = store;
        }

        protected override OctetString GetData()
        {
            var tag = _store.NodeTag;
            if (string.IsNullOrWhiteSpace(tag))
                return null;

            return new OctetString(tag);
        }
    }
}
