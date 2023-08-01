using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseCountOfRevisionDocuments : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseCountOfRevisionDocuments(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.CountOfRevisionDocuments, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetCount(database));
        }

        private static long GetCount(DocumentDatabase database)
        {
            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                return database.DocumentsStorage.RevisionsStorage.GetNumberOfRevisionDocuments(context);
            }
        }
    }
}
