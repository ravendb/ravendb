using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseAverageRequestTime : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<int>
    {
        public DatabaseAverageRequestTime(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.RequestAverageDuration, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetCount(database));
        }

        private static int GetCount(DocumentDatabase database)
        {
            return (int)database.Metrics.Requests.AverageDuration.GetRate();
        }

        public Measurement<int> GetCurrentMeasurement()
        {
            if (TryGetDatabase(out var db))
                return new(GetCount(db), MeasurementTags);
            return new(0, MeasurementTags);
        }
    }
}
