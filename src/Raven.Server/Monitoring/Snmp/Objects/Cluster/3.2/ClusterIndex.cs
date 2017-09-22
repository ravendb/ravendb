using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ClusterIndex : ScalarObjectBase<Integer32>
    {
        private readonly ServerStore _store;

        public ClusterIndex(ServerStore store)
            : base("3.2.2")
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
