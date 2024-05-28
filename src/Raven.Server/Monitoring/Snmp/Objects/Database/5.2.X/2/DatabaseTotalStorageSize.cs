using System.Diagnostics.Metrics;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseTotalStorageSize : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        public DatabaseTotalStorageSize(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.TotalStorageSize, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetTotalStorageSize(database));
        }

        private long GetTotalStorageSize(DocumentDatabase database)
        {
            var size = database.GetAllStoragesEnvironment().Sum(x => x.Environment.Stats().AllocatedDataFileSizeInBytes);
            return size / 1024L / 1024L;
        }

        public Measurement<long> GetCurrentMeasurement()
        {
            if (TryGetDatabase(out var database))
                return new(GetTotalStorageSize(database), MeasurementTags);

            return new(default, MeasurementTags);
        }
    }
}
