// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseNumberOfFaultyIndexes : DatabaseBase<Integer32>, ITaggedMetricInstrument<int>
    {
        public TotalDatabaseNumberOfFaultyIndexes(ServerStore serverStore, KeyValuePair<string, object> nodeTag = default)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfFaultyIndexes, nodeTag)
        {
        }
        
        

        protected override Integer32 GetData()
        {
            return new Integer32(GetCurrentValue().Value);
        }

        public Measurement<int> GetCurrentValue()
        {
            var count = 0;
            foreach (var database in GetLoadedDatabases())
                count += GetCountSafely(database, DatabaseNumberOfFaultyIndexes.GetCount);
            return new (count, MeasurementTag);
        }
    }
}
