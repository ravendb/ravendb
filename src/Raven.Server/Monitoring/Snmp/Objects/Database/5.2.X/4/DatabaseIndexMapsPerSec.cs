using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseIndexMapsPerSec : DatabaseIndexScalarObjectBase<Gauge32>
    {
        public DatabaseIndexMapsPerSec(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.MapsPerSec)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            return new Gauge32((int)(index.MapsPerSec?.OneMinuteRate ?? 0));
        }
    }
}
