using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Documents
{
    public class DatabaseLoadedCount : ScalarObjectBase<Integer32>
    {
        private readonly DatabasesLandlord _landlord;

        public DatabaseLoadedCount(DatabasesLandlord landlord)
            : base("5.1.2")
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
