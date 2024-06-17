using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseAlerts : DatabaseScalarObjectBase<Integer32>
    {
        public DatabaseAlerts(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.Alerts, index)
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            return new Integer32((int)database.NotificationCenter.GetAlertCount());
        }
    }
}
