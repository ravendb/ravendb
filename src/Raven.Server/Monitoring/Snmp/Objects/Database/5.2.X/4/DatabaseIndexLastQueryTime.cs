using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexLastQueryTime : DatabaseIndexScalarObjectBase<OctetString>, ITaggedMetricInstrument<long>
    {
        public DatabaseIndexLastQueryTime(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex, string nodeTag = null)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.LastQueryTime, nodeTag)
        {
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            var stats = index.GetStats();
            if (stats.LastQueryingTime.HasValue)
                return new OctetString(stats.LastQueryingTime.ToString());

            return null;
        }
        
        public Measurement<long> GetCurrentValue()
        {
            if (TryGetIndex(out var index))
            {
                var stats = index.GetStats();
                if (stats.LastQueryingTime.HasValue)
                    return new (stats.LastQueryingTime.Value.ToUniversalTime().Ticks, MeasurementTags);
            }

            return default;
        }
    }
}
