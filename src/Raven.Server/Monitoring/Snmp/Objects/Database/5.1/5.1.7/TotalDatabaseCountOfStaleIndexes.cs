using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseCountOfStaleIndexes : DatabaseBase<Gauge32>
    {
        public TotalDatabaseCountOfStaleIndexes(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfStaleIndexes)
        {
        }

        protected override Gauge32 GetData()
        {
            var count = 0;
            foreach (var database in GetLoadedDatabases())
                count += GetCountSafely(database, GetCount);

            return new Gauge32(count);
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
    }
}
