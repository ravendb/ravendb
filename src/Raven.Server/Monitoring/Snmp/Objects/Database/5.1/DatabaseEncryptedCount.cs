using System.Linq;
using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseEncryptedCount : DatabaseBase<Integer32>
    {
        public DatabaseEncryptedCount(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.EncryptedCount)
        {
        }

        protected override Integer32 GetData()
        {
            return new Integer32(GetCount());
        }

        private int GetCount()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return GetDatabases(context).Count(x => x.IsEncrypted);
            }
        }
    }
}
