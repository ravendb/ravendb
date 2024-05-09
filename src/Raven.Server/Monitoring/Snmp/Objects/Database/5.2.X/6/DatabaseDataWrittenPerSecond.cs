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
    public sealed class DatabaseDataWrittenPerSecond : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<int>
    {
        public DatabaseDataWrittenPerSecond(string databaseName, DatabasesLandlord landlord, int index, string nodeTag = null)
            : base(databaseName, landlord, SnmpOids.Databases.DataWrittenPerSecond, index, nodeTag)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetCount(database));
        }

        private static int GetCount(DocumentDatabase database)
        {
            var value = database.Metrics.Docs.BytesPutsPerSec.OneMinuteRate
                        + database.Metrics.Attachments.BytesPutsPerSec.OneMinuteRate
                        + database.Metrics.Counters.BytesPutsPerSec.OneMinuteRate
                        + database.Metrics.TimeSeries.BytesPutsPerSec.OneMinuteRate;

            return (int)value;
        }

        public Measurement<int> GetCurrentValue()
        {
            var db = GetDatabase();
            var result = db != null ? GetCount(db) : 0;
            return new(result, MeasurementTags);
        }
    }
}
