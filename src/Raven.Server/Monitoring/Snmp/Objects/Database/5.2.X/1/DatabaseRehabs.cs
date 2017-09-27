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
            var record = database.ServerStore.LoadDatabaseRecord(database.Name, out _);
            return new Integer32(record.Topology?.Rehabs?.Count ?? 0);
        }
    }
}
