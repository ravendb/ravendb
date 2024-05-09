using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Exceptions.Database;
using Raven.Server.Config.Categories;
using Raven.Server.Documents;
using Raven.Server.Monitoring.Snmp.Objects.Database;

namespace Raven.Server.Monitoring.OpenTelemetry;

public class DatabaseWideMetrics : MetricsBase
{
    private const int IgnoreIndex = 0;
    private readonly DatabasesLandlord _databaseLandlord;
    private readonly string _databaseName;
    private readonly string _nodeTag;
    private readonly SemaphoreSlim _locker = new(1, 1);
    private bool _attached;
    private HashSet<string> _loadedIndexes = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Meter are shared between all indexes since index/database identification happens via measurment tag
    /// </summary>
    private static readonly Lazy<Meter> IndexesMeter = new(() => new Meter(Constants.IndexMeter, "1.0.0", [new(Constants.Tags.NodeTag, NodeTag)]));
    private static readonly Lazy<Meter> DatabasesStorageMeter = new(() => new Meter(Constants.DatabaseStorageMeter, "1.0.0", [new(Constants.Tags.NodeTag, NodeTag)]));

    public DatabaseWideMetrics(DatabasesLandlord databaseLandlord, string databaseName, string nodeTag, MonitoringConfiguration.OpenTelemetryConfiguration configuration) : base(configuration)
    {
        _databaseLandlord = databaseLandlord;
        _databaseName = databaseName;
        _nodeTag = nodeTag;

        RegisterDatabaseStorageMetricMetric();

        databaseLandlord.OnDatabaseLoaded += loadedDatabaseName =>
        {
            if (string.Equals(loadedDatabaseName, databaseName, StringComparison.OrdinalIgnoreCase) == false)
                return;
            
            Attach(force: true);
        };
        
        if (databaseLandlord.IsDatabaseLoaded(databaseName) == false)
            Attach(force: false);
    }
    
    private void Attach(bool force)
    {
        if (force == false && _attached)
            return;

        Task.Factory.StartNew(async () =>
        {
            await _locker.WaitAsync();

            try
            {
                if (force == false && _attached)
                    return;

                var database = await _databaseLandlord.TryGetOrCreateResourceStore(_databaseName);

                database.Changes.OnIndexChange += AddIndexIfNecessary;

                var indexes = database.IndexStore.GetIndexes().ToList();
                foreach (var index in indexes)
                {
                    if (_loadedIndexes.Add(index.Name) == false)
                        continue;
                    
                    RegisterIndexInstruments(index.Name);
                }

                _attached = true;
            }
            catch (DatabaseDisabledException)
            {
                // ignored
            }
            finally
            {
                _locker.Release();
            }
        });
    }

    private void AddIndexIfNecessary(IndexChange change)
    {
        if (change.Type != IndexChangeTypes.IndexAdded)
            return;
        
        Task.Factory.StartNew(async () =>
        {
            await _locker.WaitAsync();

            try
            {
                if (_loadedIndexes.Add(change.Name) == false)
                    return;

                var database = await _databaseLandlord.TryGetOrCreateResourceStore(_databaseName);
                RegisterIndexInstruments(change.Name);
            }
            finally
            {
                _locker.Release();
            }
        });
    }

     
    
    private void RegisterDatabaseStorageMetricMetric()
    {
        CreateObservableGaugeWithTags<long, DatabaseCountOfIndexes>(
            name: Constants.DatabaseWide.DatabaseCountOfIndexes,
            observeValue: new DatabaseCountOfIndexes(_databaseName, _databaseLandlord, IgnoreIndex, _nodeTag),
            description: "Documents storage allocated size in MB",
            family: Configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );
        
        CreateObservableGaugeWithTags<long, DatabaseDocumentsStorageAllocatedSize>(
            name: Constants.DatabaseWide.DatabaseDocumentsStorageAllocatedSize,
            observeValue: new DatabaseDocumentsStorageAllocatedSize(_databaseName, _databaseLandlord, IgnoreIndex, _nodeTag),
            description: "Documents storage allocated size in MB",
            family: Configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseDocumentsStorageUsedSize>(
            name: Constants.DatabaseWide.DatabaseDocumentsStorageUsedSize,
            observeValue: new DatabaseDocumentsStorageUsedSize(_databaseName, _databaseLandlord, IgnoreIndex, _nodeTag),
            description: "Documents storage used size in MB",
            family: Configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseIndexStorageAllocatedSize>(
            name: Constants.DatabaseWide.DatabaseIndexStorageAllocatedSize,
            observeValue: new DatabaseIndexStorageAllocatedSize(_databaseName, _databaseLandlord, IgnoreIndex, _nodeTag),
            description: "Index storage allocated size in MB",
            family: Configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseIndexStorageUsedSize>(
            name: Constants.DatabaseWide.DatabaseIndexStorageUsedSize,
            observeValue: new DatabaseIndexStorageUsedSize(_databaseName, _databaseLandlord, IgnoreIndex, _nodeTag),
            description: "Index storage used size in MB",
            family: Configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );
        
        CreateObservableGaugeWithTags<long, DatabaseTotalStorageSize>(
            name: Constants.DatabaseWide.DatabaseTotalStorageSize,
            observeValue: new DatabaseTotalStorageSize(_databaseName, _databaseLandlord, IgnoreIndex, _nodeTag),
            description: "Total storage size in MB",
            family: Configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseStorageDiskRemainingSpace>(
            name: Constants.DatabaseWide.DatabaseStorageDiskRemainingSpace,
            observeValue: new DatabaseStorageDiskRemainingSpace(_databaseName, _databaseLandlord, IgnoreIndex, _nodeTag),
            description: "Remaining storage disk space in MB",
            family: Configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<int, DatabaseStorageDiskIosReadOperations>(
            name: Constants.DatabaseWide.DatabaseStorageDiskIosReadOperations,
            observeValue: new DatabaseStorageDiskIosReadOperations(_databaseName, _databaseLandlord, IgnoreIndex, _nodeTag),
            description: "IO read operations per second",
            family: Configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<int, DatabaseStorageDiskIosWriteOperations>(
            name: Constants.DatabaseWide.DatabaseStorageDiskIosWriteOperations,
            observeValue: new DatabaseStorageDiskIosWriteOperations(_databaseName, _databaseLandlord, IgnoreIndex, _nodeTag),
            description: "IO write operations per second",
            family: Configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseStorageDiskReadThroughput>(
            name: Constants.DatabaseWide.DatabaseStorageDiskReadThroughput,
            observeValue: new DatabaseStorageDiskReadThroughput(_databaseName, _databaseLandlord, IgnoreIndex, _nodeTag),
            description: "Read throughput in kilobytes per second",
            family: Configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseStorageDiskWriteThroughput>(
            name: Constants.DatabaseWide.DatabaseStorageDiskWriteThroughput,
            observeValue: new DatabaseStorageDiskWriteThroughput(_databaseName, _databaseLandlord, IgnoreIndex, _nodeTag),
            description: "Write throughput in kilobytes per second",
            family: Configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseStorageDiskQueueLength>(
            name: Constants.DatabaseWide.DatabaseStorageDiskQueueLength,
            observeValue: new DatabaseStorageDiskQueueLength(_databaseName, _databaseLandlord, IgnoreIndex, _nodeTag),
            description: "Queue length",
            family: Configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );
    }

    private void RegisterIndexInstruments(string indexName)
    {
        CreateObservableGaugeWithTags<byte, DatabaseIndexExists>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexExists,
            observeValue: new DatabaseIndexExists(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Indicates if index exists",
            family: Configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<byte, DatabaseIndexPriority>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexPriority,
            observeValue: new DatabaseIndexPriority(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Index priority",
            family: Configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<byte, DatabaseIndexState>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexState,
            observeValue: new DatabaseIndexState(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Index state",
            family: Configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<int, DatabaseIndexErrors>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexErrors,
            observeValue: new DatabaseIndexErrors(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Number of index errors",
            family: Configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<long, DatabaseIndexLastQueryTime>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexLastQueryTime,
            observeValue: new DatabaseIndexLastQueryTime(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Last query time",
            family: Configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<long, DatabaseIndexLastIndexingTime>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexLastIndexingTime,
            observeValue: new DatabaseIndexLastIndexingTime(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Index indexing time",
            family: Configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<long, DatabaseIndexTimeSinceLastQuery>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexTimeSinceLastQuery,
            observeValue: new DatabaseIndexTimeSinceLastQuery(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Time since last query",
            family: Configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<long, DatabaseIndexTimeSinceLastIndexing>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexTimeSinceLastIndexing,
            observeValue: new DatabaseIndexTimeSinceLastIndexing(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Time since last indexing",
            family: Configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<byte, DatabaseIndexLockMode>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexLockMode,
            observeValue: new DatabaseIndexLockMode(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Index lock mode",
            family: Configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<byte, DatabaseIndexIsInvalid>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexIsInvalid,
            observeValue: new DatabaseIndexIsInvalid(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Indicates if index is invalid",
            family: Configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<byte, DatabaseIndexStatus>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexStatus,
            observeValue: new DatabaseIndexStatus(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Index status",
            family: Configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<int, DatabaseIndexMapsPerSec>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexMapsPerSec,
            observeValue: new DatabaseIndexMapsPerSec(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Number of maps per second (one minute rate)",
            family: Configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<int, DatabaseIndexReducesPerSec>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexReducesPerSec,
            observeValue: new DatabaseIndexReducesPerSec(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Number of reduces per second (one minute rate)",
            family: Configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<byte, DatabaseIndexType>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexType,
            observeValue: new DatabaseIndexType(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex, _nodeTag),
            description: "Index type",
            family: Configuration.IndexInstruments,
            IndexesMeter);
    }
}
