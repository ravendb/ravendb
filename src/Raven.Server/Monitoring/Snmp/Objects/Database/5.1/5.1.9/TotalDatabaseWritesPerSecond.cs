using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseWritesPerSecond(ServerStore serverStore) : DatabaseBase<Gauge32>(serverStore, SnmpOids.Databases.General.TotalWritesPerSecond), IMetricInstrument<int>
    {
        private int Value
        {
            get
            {
                var count = 0;
                foreach (var database in GetLoadedDatabases())
                    count += GetCountSafely(database, GetCount);
                return count;
            }
        }

        public int GetCurrentMeasurement() => Value;
        
        protected override Gauge32 GetData()
        {
            return new Gauge32(GetCurrentMeasurement());
        }

        private static int GetCount(DocumentDatabase database)
        {
            var value = database.Metrics.Docs.PutsPerSec.OneMinuteRate
                        + database.Metrics.Attachments.PutsPerSec.OneMinuteRate
                        + database.Metrics.Counters.PutsPerSec.OneMinuteRate
                        + database.Metrics.TimeSeries.PutsPerSec.OneMinuteRate;

            return (int)value;
        }
    }
}
