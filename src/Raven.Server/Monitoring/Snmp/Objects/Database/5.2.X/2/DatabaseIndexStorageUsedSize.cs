using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexStorageUsedSize : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseIndexStorageUsedSize(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.IndexStorageUsedSize, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            var size = database.IndexStore
                .GetIndexes()
                .Sum(x => x._indexStorage.Environment().Stats().UsedDataFileSizeInBytes);

            return new Gauge32(size / 1024L / 1024L);
        }
    }
}
