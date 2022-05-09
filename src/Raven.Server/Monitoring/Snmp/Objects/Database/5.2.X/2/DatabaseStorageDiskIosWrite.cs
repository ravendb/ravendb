using System;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.ServerWide;

namespace Raven.Server.Monitoring.Snmp.Objects.Database
{
    public class DatabaseStorageDiskIosWrite : DatabaseScalarObjectBase<Gauge32>
    {
        private static readonly Gauge32 Empty = new Gauge32(-1);

        public DatabaseStorageDiskIosWrite(string databaseName, DatabasesLandlord landlord, int index)
            : base(databaseName, landlord, SnmpOids.Databases.StorageDiskIoWrite, index)
        {
        }

        protected override Gauge32 GetData(DocumentDatabase database)
        {
            if (database.Configuration.Core.RunInMemory)
                return Empty;

            var result = database.ServerStore.Server.DiskStatsGetter.Get(database.DocumentsStorage.Environment.Options.DriveInfoByPath?.Value.BasePath.DriveName);
            return result == null ? Empty : new Gauge32((int)Math.Round(result.IoWriteOperations));
        }
    }
}
