using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseMapReduceIndexReducedPerSecond : DatabaseBase<Gauge32>
    {
        public TotalDatabaseMapReduceIndexReducedPerSecond(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalMapReduceIndexReducedPerSecond)
        {
        }

        protected override Gauge32 GetData()
        {
            var count = 0;
            foreach (var database in GetLoadedDatabases())
                count += GetCountSafely(database, GetCount);

            return new Gauge32(count);
        }

        private static int GetCount(DocumentDatabase database)
        {
            return (int)database.Metrics.MapReduceIndexes.ReducedPerSec.OneMinuteRate;
        }
    }
}
