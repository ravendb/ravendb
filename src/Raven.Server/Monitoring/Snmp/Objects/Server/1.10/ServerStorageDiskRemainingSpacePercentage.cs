using System;
using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    public class ServerStorageDiskRemainingSpacePercentage : ScalarObjectBase<Gauge32>
    {
        private readonly ServerStore _store;

        public ServerStorageDiskRemainingSpacePercentage(ServerStore store)
            : base(SnmpOids.Server.StorageDiskRemainingSpacePercentage)
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

            var total = Convert.ToDecimal(result.TotalSize.GetValue(SizeUnit.Megabytes));
            var totalFree = Convert.ToDecimal(result.TotalFreeSpace.GetValue(SizeUnit.Megabytes));
            var percentage = Convert.ToInt32(Math.Round((totalFree / total) * 100, 0, MidpointRounding.ToEven));

            return new Gauge32(percentage);
        }
    }
}
