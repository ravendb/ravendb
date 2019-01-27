using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseIndexingErrors : DatabaseScalarObjectBase<Integer32>
    {
        public DatabaseIndexingErrors(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.IndexingErrors, index)
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            var indexes = database.IndexStore.GetIndexes().ToList();

            var count = 0L;
            foreach (var index in indexes)
                count += index.GetErrorCount();

            return new Integer32((int)count);
        }
    }
}

