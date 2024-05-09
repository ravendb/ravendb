using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseWritesPerSecond : DatabaseBase<Gauge32>, ITaggedMetricInstrument<int>
    {
        public TotalDatabaseWritesPerSecond(ServerStore serverStore, KeyValuePair<string, object> nodeTag = default)
            : base(serverStore, SnmpOids.Databases.General.TotalWritesPerSecond, nodeTag)
        {
        }

        protected override Gauge32 GetData()
        {
            return new Gauge32(GetCurrentValue().Value);
        }

        private static int GetCount(DocumentDatabase database)
        {
            var value = database.Metrics.Docs.PutsPerSec.OneMinuteRate
                        + database.Metrics.Attachments.PutsPerSec.OneMinuteRate
                        + database.Metrics.Counters.PutsPerSec.OneMinuteRate
                        + database.Metrics.TimeSeries.PutsPerSec.OneMinuteRate;

            return (int)value;
        }

        public Measurement<int> GetCurrentValue()
        {
            var count = 0;
            foreach (var database in GetLoadedDatabases())
                count += GetCountSafely(database, GetCount);
            return new(count, MeasurementTag);
        }
    }
}
