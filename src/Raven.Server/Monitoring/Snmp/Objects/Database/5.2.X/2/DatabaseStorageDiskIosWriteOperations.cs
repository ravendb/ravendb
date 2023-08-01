using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public sealed class DatabaseStorageDiskIosWriteOperations : DatabaseScalarObjectBase<Gauge32>
    {
        public DatabaseStorageDiskIosWriteOperations(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.StorageDiskIoWriteOperations, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            if (database.Configuration.Core.RunInMemory)
                return null;

            var result = database.ServerStore.Server.DiskStatsGetter.Get(database.DocumentsStorage.Environment.Options.DriveInfoByPath?.Value.BasePath.DriveName);
            return result == null ? null : new Gauge32((int)Math.Round(result.IoWriteOperations));
        }
    }
}
