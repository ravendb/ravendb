using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexMapsPerSec : DatabaseIndexScalarObjectBase<Gauge32>, ITaggedMetricInstrument<int>
    {
        public DatabaseIndexMapsPerSec(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex, string nodeTag = null)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.MapsPerSec, nodeTag)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            return new Gauge32((int)(index.MapsPerSec?.OneMinuteRate ?? 0));
        }

        public Measurement<int> GetCurrentValue()
        {
            if (TryGetIndex(out var index))
                return new((int)(index.MapsPerSec?.OneMinuteRate ?? 0), MeasurementTags);

            return default;
        }
    }
}
