// -----------------------------------------------------------------------
//  <copyright file="DatabaseRequestsPerSecond.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseRequestsPerSecond : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<int>
    {
        public DatabaseRequestsPerSecond(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.RequestsPerSecond, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetCount(database));
        }

        private static int GetCount(DocumentDatabase database)
        {
            return (int)database.Metrics.Requests.RequestsPerSec.OneMinuteRate;
        }

        public Measurement<int> GetCurrentMeasurement()
        {
            if (TryGetDatabase(out var db))
                return new(GetCount(db), MeasurementTags);
            return new(0, MeasurementTags);
        }
    }
}
