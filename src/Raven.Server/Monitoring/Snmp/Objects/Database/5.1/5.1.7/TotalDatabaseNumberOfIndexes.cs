// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseNumberOfIndexes : DatabaseBase<Integer32>, ITaggedMetricInstrument<int>
    {
        public TotalDatabaseNumberOfIndexes(ServerStore serverStore, KeyValuePair<string, object> nodeTag = default)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfIndexes, nodeTag)
        {
        }

        protected override Integer32 GetData()
        {
            return new Integer32(GetCurrentValue().Value);
        }

        private static int GetCount(DocumentDatabase database)
        {
            return (int)database.IndexStore.Count;
        }

        public Measurement<int> GetCurrentValue()
        {
            var count = 0;
            foreach (var database in GetLoadedDatabases())
                count += GetCountSafely(database, GetCount);
            return new(count, MeasurementTag);
        }
    }
}
