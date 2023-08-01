// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class TotalDatabaseNumberOfIndexes : DatabaseBase<Integer32>
    {
        public TotalDatabaseNumberOfIndexes(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfIndexes)
        {
        }

        protected override Integer32 GetData()
        {
            var count = 0;
            foreach (var database in GetLoadedDatabases())
                count += GetCountSafely(database, GetCount);

            return new Integer32(count);
        }

        private static int GetCount(DocumentDatabase database)
        {
            return (int)database.IndexStore.Count;
        }
    }
}
