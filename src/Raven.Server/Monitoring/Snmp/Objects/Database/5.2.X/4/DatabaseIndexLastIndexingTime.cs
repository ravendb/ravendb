using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexLastIndexingTime : DatabaseIndexScalarObjectBase<OctetString>, ITaggedMetricInstrument<long>
    {
        public DatabaseIndexLastIndexingTime(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex, string nodeTag = null)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.LastIndexingTime, nodeTag)
        {
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            var stats = index.GetStats();
            if (stats.LastIndexingTime.HasValue)
                return new OctetString(stats.LastIndexingTime.ToString());

            return null;
        }

        public Measurement<long> GetCurrentValue()
        {
            if (TryGetIndex(out var index))
            {
                var stats = index.GetStats();
                if (stats.LastIndexingTime.HasValue)
                    return new (stats.LastIndexingTime.Value.ToUniversalTime().Ticks, MeasurementTags);
            }

            return new(default, MeasurementTags);
        }
    }
}
