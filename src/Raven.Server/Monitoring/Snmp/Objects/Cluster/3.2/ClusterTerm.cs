using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Cluster
{
    public sealed class ClusterTerm : ScalarObjectBase<Integer32>, IMetricInstrument<long>
    {
        private readonly ServerStore _store;

        public ClusterTerm(ServerStore store)
            : base(SnmpOids.Cluster.Term)
        {
            _store = store;
        }

        protected override Integer32 GetData()
        {
            var term = _store.Engine.CurrentTerm;
            return new Integer32((int)term);
        }

        public long GetCurrentMeasurement()
        {
            return _store.Engine.CurrentTerm;
        }
    }
}
