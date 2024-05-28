using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexTimeSinceLastIndexing : DatabaseIndexScalarObjectBase<TimeTicks>, ITaggedMetricInstrument<long>
    {
        public DatabaseIndexTimeSinceLastIndexing(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.TimeSinceLastIndexing)
        {
        }

        protected override TimeTicks GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            var stats = index.GetStats();

            if (stats.LastIndexingTime.HasValue)
                return SnmpValuesHelper.TimeSpanToTimeTicks(SystemTime.UtcNow - stats.LastIndexingTime.Value);

            return null;
        }

        public Measurement<long> GetCurrentMeasurement()
        {
            if (TryGetIndex(out var index))
            {
                var stats = index.GetStats();

                if (stats.LastIndexingTime.HasValue)
                {
                    return new((SystemTime.UtcNow - stats.LastIndexingTime.Value).Ticks, MeasurementTags);
                }
            }

            return default;
        }
    }
}
