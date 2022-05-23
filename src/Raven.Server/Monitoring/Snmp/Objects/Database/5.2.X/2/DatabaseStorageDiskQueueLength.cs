using Lextm.SharpSnmpLib;
using Raven.Server.Documents;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public class DatabaseStorageDiskQueueLength : DatabaseScalarObjectBase<Gauge32>
{
    private static readonly Gauge32 Empty = new Gauge32(-1);

    public DatabaseStorageDiskQueueLength(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.StorageDiskQueueLength, index)
    {
    }

    protected override Gauge32 GetData(DocumentDatabase database)
    {
        if (database.Configuration.Core.RunInMemory)
            return Empty;
            
        var result = database.ServerStore.Server.DiskStatsGetter.Get(database.DocumentsStorage.Environment.Options.DriveInfoByPath?.Value.BasePath.DriveName);
        return result == null || result.QueueLength.HasValue == false ? Empty : new Gauge32(result.QueueLength.Value);
    }
}
