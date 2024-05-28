using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseRehabs : DatabaseScalarObjectBase<Integer32>, ITaggedMetricInstrument<int>
    {
        public DatabaseRehabs(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.Rehabs, index)
        {
        }

        private int Value(DocumentDatabase database)
        {
            var topology = database.ServerStore.LoadDatabaseTopology(database.Name);
            return topology?.Rehabs?.Count ?? 0;
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            return new Integer32(Value(database));
        }

        public Measurement<int> GetCurrentMeasurement()
        {
            if (TryGetDatabase(out var database))
                return new(Value(database), MeasurementTags);
            return new (0, MeasurementTags);
        }
    }
}
