using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseMapReduceIndexReducedPerSecond : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseMapReduceIndexReducedPerSecond(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.MapReduceIndexReducedPerSecond, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetCount(database));
        }

        private static int GetCount(DocumentDatabase database)
        {
            return (int)database.Metrics.MapReduceIndexes.ReducedPerSec.OneMinuteRate;
        }
    }
}
