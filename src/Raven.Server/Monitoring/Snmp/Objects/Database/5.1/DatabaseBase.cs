using System;
using System.Collections.Generic;
using Lextm.SharpSnmpLib;
using Raven.Client;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public abstract class DatabaseBase<TData> : ScalarObjectBase<TData> where TData : ISnmpData
    {
        protected readonly ServerStore ServerStore;
        
        protected DatabaseBase(ServerStore serverStore, string dots)
            : base(dots)
        {
            ServerStore = serverStore;
        }

        protected virtual IEnumerable<RawDatabaseRecord> GetDatabases(TransactionOperationContext context)
        {
            foreach (var item in ServerStore.Cluster.ItemsStartingWith(context, Constants.Documents.Prefix, 0, int.MaxValue))
                yield return new RawDatabaseRecord(context, item.Value);
        }

        protected IEnumerable<DocumentDatabase> GetLoadedDatabases()
        {
            foreach (var kvp in ServerStore.DatabasesLandlord.DatabasesCache)
            {
                var databaseTask = kvp.Value;

                if (databaseTask == null || databaseTask.IsCompletedSuccessfully == false)
                    continue;

                yield return databaseTask.Result;
            }
        }

        protected static int GetCountSafely(DocumentDatabase database, Func<DocumentDatabase, int> getCount)
        {
            try
            {
                return getCount(database);
            }
            catch
            {
                // e.g. database may be unloaded already
                return 0;
            }
        }
    }
}
