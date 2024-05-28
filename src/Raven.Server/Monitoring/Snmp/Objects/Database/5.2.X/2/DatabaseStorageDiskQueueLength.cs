using System.Diagnostics.Metrics;
using Lextm.SharpSnmpLib;
using Raven.Server.Documents;
using Raven.Server.Monitoring.OpenTelemetry;

namespace Raven.Server.Monitoring.Snmp.Objects.Database;

public sealed class DatabaseStorageDiskQueueLength : DatabaseScalarObjectBase<Gauge32>, ITaggedMetricInstrument<long>
{
    public DatabaseStorageDiskQueueLength(string databaseName, DatabasesLandlord landlord, int index)
        : base(databaseName, landlord, SnmpOids.Databases.StorageDiskQueueLength, index)
    {
    }

    protected override Gauge32 GetData(DocumentDatabase database)
    {
        if (database.Configuration.Core.RunInMemory)
            return null;
            
        var result = database.ServerStore.Server.DiskStatsGetter.Get(database.DocumentsStorage.Environment.Options.DriveInfoByPath?.Value.BasePath.DriveName);
        return result == null || result.QueueLength.HasValue == false ? null : new Gauge32(result.QueueLength.Value);
    }

    public Measurement<long> GetCurrentMeasurement()
    {
        if (TryGetDatabase(out var database))
        {
            if (database.Configuration.Core.RunInMemory)
                return new(default, MeasurementTags);
            
            var result = database.ServerStore.Server.DiskStatsGetter.Get(database.DocumentsStorage.Environment.Options.DriveInfoByPath?.Value.BasePath.DriveName)?.QueueLength;

            return new(result ?? 0, MeasurementTags);
        }

        return new(default, MeasurementTags);
    }
}
