using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerStorageAllocatedSize : ScalarObjectBase<Gauge32>
    {
        private readonly RavenServer _server;

        public ServerStorageAllocatedSize(RavenServer server)
            : base(SnmpOids.Server.StorageAllocatedSize)
        {
            _server = server;
        }

        protected override Gauge32 GetData()
        {
            var stats = _server.ServerStore._env.Stats();
            return new Gauge32(stats.AllocatedDataFileSizeInBytes / 1024L / 1024L);
        }
    }
}
