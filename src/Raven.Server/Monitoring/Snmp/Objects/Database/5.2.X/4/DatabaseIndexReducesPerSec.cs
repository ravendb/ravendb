using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseIndexReducesPerSec : DatabaseIndexScalarObjectBase<Gauge32>
    {
        public DatabaseIndexReducesPerSec(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.ReducesPerSec)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            return new Gauge32((int)(index.ReducesPerSec?.OneMinuteRate ??0));
        }
    }
}
