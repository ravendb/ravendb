using Lextm.SharpSnmpLib;
using Sparrow;
using Sparrow.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerStorageDiskRemainingSpace : ScalarObjectBase<Gauge32>
    {
        private readonly RavenServer _server;
        private static readonly Gauge32 Empty = new Gauge32(-1);

        public ServerStorageDiskRemainingSpace(RavenServer server)
            : base(SnmpOids.Server.StorageDiskRemainingSpace)
        {
            _server = server;
        }

        protected override Gauge32 GetData()
        {
            if (_server.Configuration.Core.RunInMemory)
                return Empty;

            var result = DiskSpaceChecker.GetDiskSpaceInfo(_server.Configuration.Core.DataDirectory.FullPath);
            if (result == null)
                return Empty;

            return new Gauge32(result.TotalFreeSpace.GetValue(SizeUnit.Megabytes));
        }
    }
}
