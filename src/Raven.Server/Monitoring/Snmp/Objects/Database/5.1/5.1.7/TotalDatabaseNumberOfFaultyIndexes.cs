// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Diagnostics.Metrics;
using Google.Protobuf.WellKnownTypes;
using Lextm.SharpSnmpLib;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseNumberOfFaultyIndexes(ServerStore serverStore)
        : DatabaseBase<Integer32>(serverStore, SnmpOids.Databases.General.TotalNumberOfFaultyIndexes), IMetricInstrument<int>
    {
        private int Value
        {
            get
            {
                var count = 0;
                foreach (var database in GetLoadedDatabases())
                    count += GetCountSafely(database, DatabaseNumberOfFaultyIndexes.GetCount);
                return count;
            }
        }
        
        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }

        public int GetCurrentMeasurement() => Value;
    }
}
