using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseCountOfStaleIndexes : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseCountOfStaleIndexes(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.CountOfStaleIndexes, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            using (var context = QueryOperationContext.Allocate(database, needsServerContext: true))
            using (context.OpenReadTransaction())
            {
                var count = database
                    .IndexStore
                    .GetIndexes()
                    .Count(x => x.IsStale(context));

                return new Gauge32(count);
            }
        }
    }
}
