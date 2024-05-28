using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseDocumentsStorageAllocatedSize : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        public DatabaseDocumentsStorageAllocatedSize(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.DocumentsStorageAllocatedSize, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetDocumentsStorageAllocatedSize(database));
        }

        public Measurement<long> GetCurrentMeasurement()
        {
            if (TryGetDatabase(out var database))
                return new(GetDocumentsStorageAllocatedSize(database), MeasurementTags);

            return new(default, MeasurementTags);
        }

        private long GetDocumentsStorageAllocatedSize(DocumentDatabase database)
        {
            var stats = database.DocumentsStorage.Environment.Stats();
            return stats.AllocatedDataFileSizeInBytes / 1024L / 1024L;
        }
    }
}
