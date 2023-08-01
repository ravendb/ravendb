using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseLoadedCount : ScalarObjectBase<Integer32>
    {
        private readonly DatabasesLandlord _landlord;

        public DatabaseLoadedCount(DatabasesLandlord landlord)
            : base(SnmpOids.Databases.General.LoadedCount)
        {
            _landlord = landlord;
        }

        protected override Integer32 GetData()
        {
            return new Integer32(GetCount(_landlord));
        }

        private static int GetCount(DatabasesLandlord landlord)
        {
            return landlord.DatabasesCache.Count;
        }
    }
}
