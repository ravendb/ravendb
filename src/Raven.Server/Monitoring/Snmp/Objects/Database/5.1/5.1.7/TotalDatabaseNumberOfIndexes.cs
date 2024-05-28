// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseNumberOfIndexes(ServerStore serverStore)
        : DatabaseBase<Integer32>(serverStore, SnmpOids.Databases.General.TotalNumberOfIndexes), IMetricInstrument<int>
    {
        private int Value
        {
            get
            {
                var count = 0;
                foreach (var database in GetLoadedDatabases())
                    count += GetCountSafely(database, GetCount);
                return count;
            }
        }
        
        protected override Integer32 GetData()
        {
            return new Integer32(Value);
        }
        
        public int GetCurrentMeasurement() => Value;
        
        private static int GetCount(DocumentDatabase database)
        {
            return (int)database.IndexStore.Count;
        }

    }
}
