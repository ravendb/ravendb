using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseRehabs : DatabaseScalarObjectBase<Integer32>
    {
        public DatabaseRehabs(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.Rehabs, index)
        {
        }

        protected override Integer32 GetData(DocumentDatabase database)
        {
            var topology = database.ServerStore.LoadDatabaseTopology(database.Name);
            return new Integer32(topology?.Rehabs?.Count ?? 0);
        }
    }
}
