using Lextm.SharpSnmpLib;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerStorageTotalSize : ScalarObjectBase<Gauge32>
    {
        private readonly RavenServer _server;

        public ServerStorageTotalSize(RavenServer server)
            : base(SnmpOids.Server.StorageTotalSize)
        {
            _server = server;
        }

        protected override Gauge32 GetData()
        {
            var size = _server.ServerStore._env.Stats().AllocatedDataFileSizeInBytes;
            return new Gauge32(size / 1024L / 1024L);
        }
    }
}
