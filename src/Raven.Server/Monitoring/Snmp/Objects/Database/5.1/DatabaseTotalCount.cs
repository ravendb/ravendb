using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Client;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseTotalCount : ScalarObjectBase<Integer32>
    {
        private readonly ServerStore _serverStore;

        public DatabaseTotalCount(ServerStore serverStore)
            : base(SnmpOids.Databases.General.TotalCount)
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
                var items = serverStore.Cluster.ItemsStartingWith(context, Constants.Documents.Prefix, 0, long.MaxValue);
                return items.Count();
            }
        }
    }
}
