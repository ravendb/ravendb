// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseWritesPerSecond : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<int>
    {
        public DatabaseWritesPerSecond(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.WritesPerSecond, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetCount(database));
        }

        private static int GetCount(DocumentDatabase database)
        {
            var value = database.Metrics.Docs.PutsPerSec.OneMinuteRate
                        + database.Metrics.Attachments.PutsPerSec.OneMinuteRate
                        + database.Metrics.Counters.PutsPerSec.OneMinuteRate
                        + database.Metrics.TimeSeries.PutsPerSec.OneMinuteRate;

            return (int)value;
        }

        public Measurement<int> GetCurrentMeasurement()
        {
            if (TryGetDatabase(out var db))
                return new(GetCount(db), MeasurementTags);
            return new(0, MeasurementTags);
        }
    }
}
