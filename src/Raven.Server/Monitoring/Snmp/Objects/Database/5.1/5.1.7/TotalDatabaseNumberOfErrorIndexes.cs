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
    public sealed class TotalDatabaseNumberOfErrorIndexes : DatabaseBase<Integer32>, ITaggedMetricInstrument<int>
    {
        public TotalDatabaseNumberOfErrorIndexes(ServerStore serverStore, KeyValuePair<string, object> nodeTag = default)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfErrorIndexes, nodeTag)
        {
        }

        private int Value
        {
            get
            {
                var count = 0;
                foreach (var database in GetLoadedDatabases())
                    count += GetCountSafely(database, DatabaseNumberOfErrorIndexes.GetCount);
                return count;
            }
        }

        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public Measurement<int> GetCurrentValue()
        {
            return new(Value, MeasurementTag);
        }
    }
}
