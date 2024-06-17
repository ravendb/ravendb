using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    internal sealed class DatabaseUpTime : DatabaseScalarObjectBase<TimeTicks>
    {
        public DatabaseUpTime(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.UpTime, index)
        {
        }

        protected override TimeTicks GetData(DocumentDatabase database)
        {
            return SnmpValuesHelper.TimeSpanToTimeTicks(SystemTime.UtcNow - database.StartTime);
        }
    }
}
