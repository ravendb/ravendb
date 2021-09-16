using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalDatabaseWritesPerSecond : DatabaseBase<Gauge32>
    {
        public TotalDatabaseWritesPerSecond(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalWritesPerSecond)
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
            var value = database.Metrics.Docs.PutsPerSec.OneMinuteRate
                        + database.Metrics.Attachments.PutsPerSec.OneMinuteRate
                        + database.Metrics.Counters.PutsPerSec.OneMinuteRate;

            return (int)value;
        }
    }
}
