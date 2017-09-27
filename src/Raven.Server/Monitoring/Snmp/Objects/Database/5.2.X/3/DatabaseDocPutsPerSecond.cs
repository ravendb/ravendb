using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseDocPutsPerSecond : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseDocPutsPerSecond(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.DocPutsPerSecond, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            return new Gauge32(GetCount(database));
        }

        private static int GetCount(DocumentDatabase database)
        {
            return (int)database.Metrics.Docs.PutsPerSec.OneMinuteRate;
        }
    }
}
