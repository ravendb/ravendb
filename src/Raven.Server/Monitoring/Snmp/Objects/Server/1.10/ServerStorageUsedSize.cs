using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerStorageUsedSize : ScalarObjectBase<Gauge32>
    {
        private readonly ServerStore _store;

        public ServerStorageUsedSize(ServerStore store)
            : base(SnmpOids.Server.StorageUsedSize)
        {
            _store = store;
        }

        protected override Gauge32 GetData()
        {
            var stats = _store._env.Stats();
            return new Gauge32(stats.UsedDataFileSizeInBytes / 1024L / 1024L);
        }
    }
}
