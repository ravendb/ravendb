using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseRehabs : DatabaseScalarObjectBase<Integer32>
    {
        public DatabaseRehabs(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.Rehabs, index)
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            var rawRecord = database.ServerStore.LoadRawDatabaseRecord(database.Name, out _);
            var topology = database.ServerStore.Cluster.ReadDatabaseTopology(rawRecord);
            return new Integer32(topology?.Rehabs?.Count ?? 0);
        }
    }
}
