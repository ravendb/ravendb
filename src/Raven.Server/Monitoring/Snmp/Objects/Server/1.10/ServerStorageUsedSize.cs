using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerStorageUsedSize : ScalarObjectBase<Gauge32>
    {
        private readonly RavenServer _server;

        public ServerStorageUsedSize(RavenServer server)
            : base(SnmpOids.Server.StorageUsedSize)
        {
            _server = server;
        }

        protected override Gauge32 GetData()
        {
            var stats = _server.ServerStore._env.Stats();
            return new Gauge32(stats.UsedDataFileSizeInBytes / 1024L / 1024L);
        }
    }
}
