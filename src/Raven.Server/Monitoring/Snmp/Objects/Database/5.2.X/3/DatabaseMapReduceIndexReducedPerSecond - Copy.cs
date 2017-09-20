using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseMapReduceIndexReducedPerSecond : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseMapReduceIndexReducedPerSecond(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, "5.2.{0}.3.4", index)
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
