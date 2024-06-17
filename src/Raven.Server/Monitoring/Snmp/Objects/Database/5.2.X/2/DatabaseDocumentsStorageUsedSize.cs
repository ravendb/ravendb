using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseDocumentsStorageUsedSize : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseDocumentsStorageUsedSize(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.DocumentsStorageUsedSize, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            var stats = database.DocumentsStorage.Environment.Stats();
            return new Gauge32(stats.UsedDataFileSizeInBytes / 1024L / 1024L);
        }
    }
}
