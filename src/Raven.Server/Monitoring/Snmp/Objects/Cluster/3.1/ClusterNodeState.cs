using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Cluster
{
    public class ClusterNodeState : ScalarObjectBase<OctetString>
    {
        private readonly ServerStore _store;

        public ClusterNodeState(ServerStore store)
            : base(SnmpOids.Cluster.NodeState)
        {
            _store = store;
        }

        protected override OctetString GetData()
        {
            var tag = _store.NodeTag;
            if (string.IsNullOrWhiteSpace(tag))
                return null;

            return new OctetString(_store.CurrentRachisState.ToString());
        }
    }
}
