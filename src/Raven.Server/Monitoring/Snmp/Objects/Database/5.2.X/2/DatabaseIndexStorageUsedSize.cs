using System.Diagnostics.Metrics;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexStorageUsedSize : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        public DatabaseIndexStorageUsedSize(string databaseName, DatabasesLandlord landlord, int index, string nodeTag = null)
            : base(databaseName, landlord, SnmpOids.Databases.IndexStorageUsedSize, index, nodeTag)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetIndexStorageUsedSize(database));
        }

        private long GetIndexStorageUsedSize(DocumentDatabase database)
        {
            var size = database.IndexStore
                .GetIndexes()
                .Sum(x => x._indexStorage.Environment().Stats().UsedDataFileSizeInBytes);

            return size / 1024L / 1024L;
        }

        public Measurement<long> GetCurrentValue()
        {
            if (TryGetDatabase(out var database))
                return new(GetIndexStorageUsedSize(database), MeasurementTags);

            return new(default, MeasurementTags);
        }
    }
}
