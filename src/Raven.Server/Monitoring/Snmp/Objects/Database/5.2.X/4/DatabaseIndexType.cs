using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseIndexType : DatabaseIndexScalarObjectBase<OctetString>
    {
        public DatabaseIndexType(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, "15")
        {
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            return new OctetString(index.Type.ToString());
        }
    }
}
