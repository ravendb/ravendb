using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerStorageDiskRemainingSpace : ScalarObjectBase<Gauge32>
    {
        private readonly ServerStore _store;

        public ServerStorageDiskRemainingSpace(ServerStore store)
            : base(SnmpOids.Server.StorageDiskRemainingSpace)
        {
            _store = store;
        }

        protected override Gauge32 GetData()
        {
            if (_store.Configuration.Core.RunInMemory)
                return null;

            var result = _store.Server.MetricCacher.GetValue<DiskSpaceResult>(MetricCacher.Keys.Server.DiskSpaceInfo);
            if (result == null)
                return null;

            return new Gauge32(result.TotalFreeSpace.GetValue(SizeUnit.Megabytes));
        }
    }
}
