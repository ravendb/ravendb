using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseDocumentsStorageUsedSize : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        public DatabaseDocumentsStorageUsedSize(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.DocumentsStorageUsedSize, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetDocumentsStorageUsedSize(database));
        }

        private long GetDocumentsStorageUsedSize(DocumentDatabase database)
        {
            var stats = database.DocumentsStorage.Environment.Stats();
            return stats.UsedDataFileSizeInBytes / 1024L / 1024L;
        }

        public Measurement<long> GetCurrentMeasurement()
        {
            if (TryGetDatabase(out var db))
                return new(GetDocumentsStorageUsedSize(db), MeasurementTags);

            return new(default, MeasurementTags);
        }
    }
}
