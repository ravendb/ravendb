using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseIndexLastQueryTime : DatabaseIndexScalarObjectBase<OctetString>
    {
        public DatabaseIndexLastQueryTime(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, "7")
        {
        }

        protected override OctetString GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            var stats = index.GetStats();
            if (stats.LastQueryingTime.HasValue)
                return new OctetString(stats.LastQueryingTime.ToString());

            return null;
        }
    }
}
