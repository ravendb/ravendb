using System;
using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseStorageDiskIosReadOperations : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<int>
{
    public DatabaseStorageDiskIosReadOperations(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.StorageDiskIoReadOperations, index)
    {
    }

    private int? Value(DocumentDatabase database)
    {
        if (database.Configuration.Core.RunInMemory)
            return null;
        
        var result = database.ServerStore.Server.DiskStatsGetter.Get(database.DocumentsStorage.Environment.Options.DriveInfoByPath?.Value.BasePath.DriveName);
        return (int)Math.Round(result.IoReadOperations);
    }

    protected override Gauge32 GetData(DocumentDatabase database)
    {
        var result = Value(database);
        return result == null 
            ? null 
            : new Gauge32(result.Value);
    }
    
    public Measurement<int> GetCurrentMeasurement()
    {
        if (TryGetDatabase(out var database) && Value(database) is { } result)
        {
            return new(result, MeasurementTags);
        }
        
        return new(default, MeasurementTags);
    }
}
