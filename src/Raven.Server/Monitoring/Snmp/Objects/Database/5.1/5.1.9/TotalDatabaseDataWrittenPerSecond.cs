using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseDataWrittenPerSecond(ServerStore serverStore)
        : DatabaseBase<Gauge32>(serverStore, SnmpOids.Databases.General.TotalDataWrittenPerSecond), IMetricInstrument<int>
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

        protected override Gauge32 GetData()
        {
            return new Gauge32(Value);
        }

        private static int GetCount(DocumentDatabase database)
        {
            var value = database.Metrics.Docs.BytesPutsPerSec.OneMinuteRate
                        + database.Metrics.Attachments.BytesPutsPerSec.OneMinuteRate
                        + database.Metrics.Counters.BytesPutsPerSec.OneMinuteRate
                        + database.Metrics.TimeSeries.BytesPutsPerSec.OneMinuteRate;

            return (int)value;
        }

        public int GetCurrentMeasurement() => Value;
    }
}
