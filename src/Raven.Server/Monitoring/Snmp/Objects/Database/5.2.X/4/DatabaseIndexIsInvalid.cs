using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexIsInvalid : DatabaseIndexScalarObjectBase<OctetString>
    {
        public DatabaseIndexIsInvalid(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.IsInvalid)
        {
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            var stats = index.GetStats();

            return new OctetString(stats.IsInvalidIndex.ToString());
        }
    }
}
