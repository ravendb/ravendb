// -----------------------------------------------------------------------
//  <copyright file="DatabaseIndexAttempts.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexErrors : DatabaseIndexScalarObjectBase<Integer32>, ITaggedMetricInstrument<int>
    {
        public DatabaseIndexErrors(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.Errors)
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            return new Integer32((int)index.GetErrorCount());
        }

        public Measurement<int> GetCurrentMeasurement()
        {
            if (TryGetIndex(out var index))
                return new Measurement<int>((int)index.GetErrorCount(), MeasurementTags);

            return default;
        }
    }
}
