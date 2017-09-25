using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseMapReduceIndexMappedPerSecond : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseMapReduceIndexMappedPerSecond(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.MapReduceIndexMappedPerSecond, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetCount(database));
        }

        private static int GetCount(DocumentDatabase database)
        {
            return (int)database.Metrics.MapReduceIndexes.MappedPerSec.OneMinuteRate;
        }
    }
}
