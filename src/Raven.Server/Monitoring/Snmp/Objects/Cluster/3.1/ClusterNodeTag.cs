using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Cluster
{
    public class ClusterNodeTag : ScalarObjectBase<OctetString>
    {
        private readonly ServerStore _store;

        public ClusterNodeTag(ServerStore store)
            : base(SnmpOids.Cluster.NodeTag)
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
