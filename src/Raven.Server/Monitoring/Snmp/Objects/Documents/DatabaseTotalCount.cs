// -----------------------------------------------------------------------
//  <copyright file="DatabaseTotalCount.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Documents
{
    public class DatabaseTotalCount : ScalarObjectBase<Integer32>
    {
        private readonly ServerStore _serverStore;

        public DatabaseTotalCount(ServerStore serverStore)
            : base("5.1.1")
        {
            _serverStore = serverStore;
        }

        protected override Integer32 GetData()
        {
            return new Integer32(GetCount(_serverStore));
        }

        private static int GetCount(ServerStore serverStore)
        {
            using (serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var items = serverStore.Cluster.ItemsStartingWith(context, Constants.Documents.Prefix, 0, int.MaxValue);
                return items.Count();
            }
        }
    }
}
