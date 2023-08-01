using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexStorageAllocatedSize : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseIndexStorageAllocatedSize(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.IndexStorageAllocatedSize, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            var size = database.IndexStore
                .GetIndexes()
                .Sum(x => x._indexStorage.Environment().Stats().AllocatedDataFileSizeInBytes);

            return new Gauge32(size / 1024L / 1024L);
        }
    }
}
