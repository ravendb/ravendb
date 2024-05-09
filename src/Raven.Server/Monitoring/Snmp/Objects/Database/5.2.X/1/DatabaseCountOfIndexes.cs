using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseCountOfIndexes : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
    {
        public DatabaseCountOfIndexes(string databaseName, DatabasesLandlord landlord, int index, string nodeTag = default)
            : base(databaseName, landlord, SnmpOids.Databases.CountOfIndexes, index, null)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(database.IndexStore.Count);
        }

        public Measurement<long> GetCurrentValue()
        {
            var value = GetDatabase()?.IndexStore.Count ?? 0;
            return new Measurement<long>(value, MeasurementTags);
        }
    }
}
