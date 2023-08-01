using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public sealed class ServerStorageTotalSize : ScalarObjectBase<Gauge32>
    {
        private readonly ServerStore _store;

        public ServerStorageTotalSize(ServerStore store)
            : base(SnmpOids.Server.StorageTotalSize)
        {
            _store = store;
        }

        protected override Gauge32 GetData()
        {
            var size = _store._env.Stats().AllocatedDataFileSizeInBytes;
            return new Gauge32(size / 1024L / 1024L);
        }
    }
}
