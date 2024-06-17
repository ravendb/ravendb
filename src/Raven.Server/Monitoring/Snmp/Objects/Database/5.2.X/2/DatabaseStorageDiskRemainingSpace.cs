using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Server.Utils;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseStorageDiskRemainingSpace : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseStorageDiskRemainingSpace(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.StorageDiskRemainingSpace, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            if (database.Configuration.Core.RunInMemory)
                return null;

            var result = database.MetricCacher.GetValue<DiskSpaceResult>(MetricCacher.Keys.Database.DiskSpaceInfo);
            if (result == null)
                return null;

            return new Gauge32(result.TotalFreeSpace.GetValue(SizeUnit.Megabytes));
        }
    }
}
