using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexReducesPerSec : DatabaseIndexScalarObjectBase<Gauge32>, ITaggedMetricInstrument<int>
    {
        public DatabaseIndexReducesPerSec(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.ReducesPerSec)
        {
        }

        private int Value(Index index) => (int)(index.ReducesPerSec?.OneMinuteRate ?? 0);

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            return new Gauge32(Value(index));
        }

        public Measurement<int> GetCurrentMeasurement()
        {
            if (TryGetIndex(out var index))
                return new Measurement<int>(Value(index), MeasurementTags);

            return new(default, MeasurementTags);
        }
    }
}
