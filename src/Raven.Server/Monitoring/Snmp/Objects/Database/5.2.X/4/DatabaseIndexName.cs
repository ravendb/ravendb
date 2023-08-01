using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexName : DatabaseIndexScalarObjectBase<OctetString>
    {
        private readonly OctetString _name;

        public DatabaseIndexName(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.Name)
        {
            _name = new OctetString(indexName);
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            return _name;
        }
    }
}
