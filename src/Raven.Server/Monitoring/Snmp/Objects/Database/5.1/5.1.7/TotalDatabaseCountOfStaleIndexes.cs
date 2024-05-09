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
    public sealed class TotalDatabaseCountOfStaleIndexes : DatabaseBase<Gauge32>, ITaggedMetricInstrument<int>
    {
        public TotalDatabaseCountOfStaleIndexes(ServerStore serverStore, KeyValuePair<string, object> nodeTag = default)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfStaleIndexes)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(GetCurrentValue().Value);
        }

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

        public Measurement<int> GetCurrentValue()
        {
            var count = 0;
            foreach (var database in GetLoadedDatabases())
                count += GetCountSafely(database, GetCount);
            return new (count, new KeyValuePair<string, object>("x", "y"));
        }
    }
}
