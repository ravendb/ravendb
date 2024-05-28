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
    public sealed class DatabaseNumberOfIndexes : DatabaseScalarObjectBase<Integer32>, ITaggedMetricInstrument<int>
    {
        public DatabaseNumberOfIndexes(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.NumberOfIndexes, index)
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            return new Integer32(Value(database));
        }

        private static int Value(DocumentDatabase database)
        {
            return (int)database.IndexStore.Count;
        }

        public Measurement<int> GetCurrentMeasurement()
        {
            if (TryGetDatabase(out var db))
                return new(Value(db), MeasurementTags);
            return new(0, MeasurementTags);
        }

    }
}
