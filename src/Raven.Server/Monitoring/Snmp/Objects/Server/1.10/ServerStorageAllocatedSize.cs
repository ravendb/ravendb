using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerStorageAllocatedSize : ScalarObjectBase<Gauge32>
    {
        private readonly ServerStore _store;

        public ServerStorageAllocatedSize(ServerStore store)
            : base(SnmpOids.Server.StorageAllocatedSize)
        {
            _store = store;
        }

        protected override Gauge32 GetData()
        {
            var stats = _store._env.Stats();
            return new Gauge32(stats.AllocatedDataFileSizeInBytes / 1024L / 1024L);
        }
    }
}
