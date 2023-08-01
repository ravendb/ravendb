using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexLastIndexingTime : DatabaseIndexScalarObjectBase<OctetString>
    {
        public DatabaseIndexLastIndexingTime(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.LastIndexingTime)
        {
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            var stats = index.GetStats();
            if (stats.LastIndexingTime.HasValue)
                return new OctetString(stats.LastIndexingTime.ToString());

            return null;
        }
    }
}
