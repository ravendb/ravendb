using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Cluster
{
    public sealed class ClusterIndex : ScalarObjectBase<Integer32>, IMetricInstrument<long>
    {
        private readonly ServerStore _store;

        public ClusterIndex(ServerStore store)
            : base(SnmpOids.Cluster.Index)
        {
            _store = store;
        }

        private long Value => _store.LastRaftCommitIndex;

        protected override Integer32 GetData()
        {
            return new Integer32((int)Value);
        }

        public long GetCurrentMeasurement() => Value;
    }
}
