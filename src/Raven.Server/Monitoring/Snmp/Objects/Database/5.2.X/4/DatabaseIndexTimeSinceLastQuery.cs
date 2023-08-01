using Lextm.SharpSnmpLib;
using Raven.Client.Util;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseIndexTimeSinceLastQuery : DatabaseIndexScalarObjectBase<TimeTicks>
    {
        public DatabaseIndexTimeSinceLastQuery(string databaseName, string indexName, DatabasesLandlord landlord, int databaseIndex, int indexIndex)
            : base(databaseName, indexName, landlord, databaseIndex, indexIndex, SnmpOids.Databases.Indexes.TimeSinceLastQuery)
        {
        }

        protected override TimeTicks GetData(DocumentDatabase database)
        {
            var index = GetIndex(database);
            var stats = index.GetStats();

            if (stats.LastQueryingTime.HasValue)
                return SnmpValuesHelper.TimeSpanToTimeTicks(SystemTime.UtcNow - stats.LastQueryingTime.Value);

            return null;
        }
    }
}
