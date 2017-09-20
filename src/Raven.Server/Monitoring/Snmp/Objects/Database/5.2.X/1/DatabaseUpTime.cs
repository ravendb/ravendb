using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Server
{
    internal class DatabaseUpTime : DatabaseScalarObjectBase<TimeTicks>
    {
        public DatabaseUpTime(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, "5.2.{0}.1.12", index)
        {
        }

        protected override TimeTicks GetData(DocumentDatabase database)
        {
            return new TimeTicks(SystemTime.UtcNow - database.StartTime);
        }
    }
}
