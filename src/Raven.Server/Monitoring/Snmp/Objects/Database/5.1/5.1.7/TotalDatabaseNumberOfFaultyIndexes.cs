// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseNumberOfFaultyIndexes : DatabaseBase<Integer32>
    {
        public TotalDatabaseNumberOfFaultyIndexes(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfFaultyIndexes)
        {
        }

        protected override Integer32 GetData()
        {
            var count = 0;
            foreach (var database in GetLoadedDatabases())
                count += GetCountSafely(database, DatabaseNumberOfFaultyIndexes.GetCount);

            return new Integer32(count);
        }
    }
}
