using System.Diagnostics.Metrics;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexStorageAllocatedSize : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        public DatabaseIndexStorageAllocatedSize(string databaseName, DatabasesLandlord landlord, int index, string nodeTag = null)
            : base(databaseName, landlord, SnmpOids.Databases.IndexStorageAllocatedSize, index, nodeTag)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetIndexStorageAllocatedSize(database));
        }

        private long GetIndexStorageAllocatedSize(DocumentDatabase database)
        {
            var size = database.IndexStore
                .GetIndexes()
                .Sum(x => x._indexStorage.Environment().Stats().AllocatedDataFileSizeInBytes);

            return size / 1024L / 1024L;
        }


        public Measurement<long> GetCurrentValue()
        {
            if (TryGetDatabase(out var database))
                return new(GetIndexStorageAllocatedSize(database), MeasurementTags);

            return new(default, MeasurementTags);
        }
    }
}
