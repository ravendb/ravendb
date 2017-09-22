using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseIndexEtag : DatabaseIndexScalarObjectBase<Integer32>
    {
        public DatabaseIndexEtag(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, "3")
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            return new Integer32((int)index.Etag);
        }
    }
}
