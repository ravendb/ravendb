using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseDataWrittenPerSecond : DatabaseBase<Gauge32>
    {
        public TotalDatabaseDataWrittenPerSecond(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalDataWrittenPerSecond)
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
            var value = database.Metrics.Docs.BytesPutsPerSec.OneMinuteRate
                        + database.Metrics.Attachments.BytesPutsPerSec.OneMinuteRate
                        + database.Metrics.Counters.BytesPutsPerSec.OneMinuteRate
                        + database.Metrics.TimeSeries.BytesPutsPerSec.OneMinuteRate;

            return (int)value;
        }
    }
}
