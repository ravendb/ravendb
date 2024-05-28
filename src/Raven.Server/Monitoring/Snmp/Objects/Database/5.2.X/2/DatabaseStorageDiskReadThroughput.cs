using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;
using Sparrow;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseStorageDiskReadThroughput : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
{
    public DatabaseStorageDiskReadThroughput(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.StorageDiskReadThroughput, index)
    {
    }

    private long? Value(DocumentDatabase database)
    {
        if (database.Configuration.Core.RunInMemory)
            return null;
            
        var result = database.ServerStore.Server.DiskStatsGetter.Get(database.DocumentsStorage.Environment.Options.DriveInfoByPath?.Value.BasePath.DriveName);

        return result?.ReadThroughput.GetValue(SizeUnit.Kilobytes);
    }

    protected override Gauge32 GetData(DocumentDatabase database)
    {
        if (Value(database) is { } result)
            return new Gauge32(result);

        return null;
    }
    
    public Measurement<long> GetCurrentMeasurement()
    {
        if (TryGetDatabase(out var database) && Value(database) is { } result)
            return new(result, MeasurementTags);
        
        return new(default, MeasurementTags);
    }
}
