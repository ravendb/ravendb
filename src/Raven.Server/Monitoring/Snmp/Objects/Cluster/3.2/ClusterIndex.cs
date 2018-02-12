using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Cluster
{
    public class ClusterIndex : ScalarObjectBase<Integer32>
    {
        private readonly ServerStore _store;

        public ClusterIndex(ServerStore store)
            : base(SnmpOids.Cluster.Index)
        {
            _store = store;
        }

        protected override Integer32 GetData()
        {
            var index = _store.LastRaftCommitIndex;
            return new Integer32((int)index);
        }
    }
}
