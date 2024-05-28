using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseAlerts : DatabaseScalarObjectBase<Integer32>, ITaggedMetricInstrument<int>
    {
        public DatabaseAlerts(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.Alerts, index)
        {
        }

        private int Value(DocumentDatabase database) => (int)database.NotificationCenter.GetAlertCount();
        
        protected override Integer32 GetData(DocumentDatabase database)
        {
            return new Integer32(Value(database));
        }

        public Measurement<int> GetCurrentMeasurement()
        {
            if (TryGetDatabase(out var db))
                return new(Value(db), MeasurementTags);

            return new(0, MeasurementTags);
        }
    }
}
