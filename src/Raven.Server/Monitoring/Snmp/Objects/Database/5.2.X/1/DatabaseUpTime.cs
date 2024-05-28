using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    internal sealed class DatabaseUpTime : DatabaseScalarObjectBase<TimeTicks>, ITaggedMetricInstrument<long>
    {
        public DatabaseUpTime(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.UpTime, index)
        {
        }

        protected override TimeTicks GetData(DocumentDatabase database)
        {
            return SnmpValuesHelper.TimeSpanToTimeTicks(SystemTime.UtcNow - database.StartTime);
        }

        public Measurement<long> GetCurrentMeasurement()
        {
            if (TryGetDatabase(out var db))
                return new((SystemTime.UtcNow - db.StartTime).Ticks, MeasurementTags);
            return new(0, MeasurementTags);
        }
    }
}
