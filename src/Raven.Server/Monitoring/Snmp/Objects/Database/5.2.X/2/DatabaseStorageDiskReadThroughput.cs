using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseStorageDiskReadThroughput : DatabaseScalarObjectBase<Gauge32>
{
    public DatabaseStorageDiskReadThroughput(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.StorageDiskReadThroughput, index)
    {
    }

    protected override Gauge32 GetData(DocumentDatabase database)
    {
        if (database.Configuration.Core.RunInMemory)
            return null;
            
        var result = database.ServerStore.Server.DiskStatsGetter.Get(database.DocumentsStorage.Environment.Options.DriveInfoByPath?.Value.BasePath.DriveName);
        return result == null ? null : new Gauge32(result.ReadThroughput.GetValue(SizeUnit.Kilobytes));
    }
}
