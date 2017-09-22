using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ClusterNodeState : ScalarObjectBase<OctetString>
    {
        private readonly ServerStore _store;

        public ClusterNodeState(ServerStore store)
            : base("3.1.2")
        {
            _store = store;
        }

        protected override OctetString GetData()
        {
            var tag = _store.NodeTag;
            if (string.IsNullOrWhiteSpace(tag))
                return null;

            return new OctetString(_store.CurrentState.ToString());
        }
    }
}
