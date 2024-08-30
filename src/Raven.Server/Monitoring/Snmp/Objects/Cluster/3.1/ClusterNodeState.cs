using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Cluster
{
    public sealed class ClusterNodeState : ScalarObjectBase<OctetString>, IMetricInstrument<byte>
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

        public byte GetCurrentMeasurement()
        {
            var tag = _store.NodeTag;
            if (string.IsNullOrWhiteSpace(tag))
                return byte.MaxValue;

            return (byte)(int)_store.CurrentRachisState;
        }
    }
}
