using Lextm.SharpSnmpLib;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class TotalNumberOfIndexingErrors : DatabaseBase<Integer32>
    {
        public TotalNumberOfIndexingErrors(ServerStore serverStore)
            : base(serverStore, SnmpOids.Databases.General.TotalNumberOfIndexingErrors)
        {
        }

        protected override Integer32 GetData()
        {
            var count = 0;
            foreach (var database in GetLoadedDatabases())
                count += GetCountSafely(database, DatabaseIndexingErrors.GetCount);

            return new Integer32(count);
        }
    }
}