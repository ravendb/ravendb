// -----------------------------------------------------------------------
//  <copyright file="DatabaseOpenedCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalDatabaseNumberOfErrorIndexes : DatabaseBase<Integer32>
    {
        public TotalDatabaseNumberOfErrorIndexes(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfErrorIndexes)
        {
        }

        protected override Integer32 GetData()
        {
            var count = 0;
            foreach (var database in GetLoadedDatabases())
                count += GetCount(database);

            return new Integer32(count);
        }

        private static int GetCount(DocumentDatabase database)
        {
            return database
                .IndexStore
                .GetIndexes()
                .Count(x => x.State == IndexState.Error);
        }
    }
}
