using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseCountOfStaleIndexes(ServerStore serverStore)
        : DatabaseBase<Gauge32>(serverStore, SnmpOids.Databases.General.TotalNumberOfStaleIndexes), IMetricInstrument<int>
    {
        private int Value
        {
            get
            {
                var count = 0;
                foreach (var database in GetLoadedDatabases())
                    count += GetCountSafely(database, GetCount);
                return count;
            }
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        public int GetCurrentMeasurement() => Value;
        
        private static int GetCount(DocumentDatabase database)
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
    }
}
