using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ClusterTerm : ScalarObjectBase<Integer32>
    {
        private readonly ServerStore _store;

        public ClusterTerm(ServerStore store)
            : base("3.2.1")
        {
            _store = store;
        }

        protected override Integer32 GetData()
        {
            var term = _store.Engine.CurrentTerm;
            return new Integer32((int)term);
        }
    }
}
