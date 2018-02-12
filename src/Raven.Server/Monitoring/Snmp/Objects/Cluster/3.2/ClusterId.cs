using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Cluster
{
    public class ClusterId : ScalarObjectBase<OctetString>
    {
        private readonly ServerStore _store;

        public ClusterId(ServerStore store)
            : base(SnmpOids.Cluster.Id)
        {
            _store = store;
        }

        protected override OctetString GetData()
        {
            var clusterId = _store.Engine.ClusterId;
            if (string.IsNullOrWhiteSpace(clusterId))
                return null;

            return new OctetString(clusterId);
        }
    }
}
