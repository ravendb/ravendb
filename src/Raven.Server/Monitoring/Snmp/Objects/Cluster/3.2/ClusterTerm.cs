using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Cluster
{
    public sealed class ClusterTerm : ScalarObjectBase<Integer32>
    {
        private readonly ServerStore _store;

        public ClusterTerm(ServerStore store)
            : base(SnmpOids.Cluster.Term)
        {
            _store = store;
        }

        protected override Integer32 GetData()
        {
            var term = _store.Engine.CurrentCommittedState.Term;
            return new Integer32((int)term);
        }
    }
}
