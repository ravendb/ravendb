using System;
using System.Diagnostics.Metrics;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexingErrors : DatabaseScalarObjectBase<Integer32>, ITaggedMetricInstrument<int>
    {
        public DatabaseIndexingErrors(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.IndexingErrors, index)
        {
        }

        private int Value(DocumentDatabase database)
        {
            var indexes = database.IndexStore.GetIndexes().ToList();

            var count = 0;
            foreach (var index in indexes)
                count += (int)index.GetErrorCount();

            return count;
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            
            return new Integer32(Value(database));
        }

        public Measurement<int> GetCurrentMeasurement()
        {
            if (TryGetDatabase(out var db))
                return new(Value(db), MeasurementTags);
            return new(0, MeasurementTags);
        }
    }
}

