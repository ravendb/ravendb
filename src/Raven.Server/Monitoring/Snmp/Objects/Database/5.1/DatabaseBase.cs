using System.Collections.Generic;
using Lextm.SharpSnmpLib;
using Raven.Client;
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

        protected IEnumerable<RawDatabaseRecord> GetDatabases(TransactionOperationContext context)
        {
            foreach (var item in ServerStore.Cluster.ItemsStartingWith(context, Constants.Documents.Prefix, 0, int.MaxValue))
                yield return new RawDatabaseRecord(context, item.Value);
        }
    }
}
