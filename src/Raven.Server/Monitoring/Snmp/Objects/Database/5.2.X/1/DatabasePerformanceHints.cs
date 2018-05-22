using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabasePerformanceHints : DatabaseScalarObjectBase<Integer32>
    {
        public DatabasePerformanceHints(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.PerformanceHints, index)
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            return new Integer32((int)database.NotificationCenter.GetPerformanceHintCount());
        }
    }
}

