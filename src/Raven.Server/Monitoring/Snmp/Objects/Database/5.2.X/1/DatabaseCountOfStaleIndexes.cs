using System.Diagnostics.Metrics;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseCountOfStaleIndexes : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<int>
    {
        public DatabaseCountOfStaleIndexes(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.CountOfStaleIndexes, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetCountOfStaleIndexes(database));
        }

        private int GetCountOfStaleIndexes(DocumentDatabase database)
        {
            using (var context = QueryOperationContext.Allocate(database, needsServerContext: true))
            using (context.OpenReadTransaction())
            {
                var count = database
                    .IndexStore
                    .GetIndexes()
                    .Count(x => x.IsStale(context));

                return count;
            }
        }

        public Measurement<int> GetCurrentMeasurement()
        {
            var db = GetDatabase();
            var value = db == null ? 0 : GetCountOfStaleIndexes(db);
            return new Measurement<int>(value, MeasurementTags);
        }
    }
}
