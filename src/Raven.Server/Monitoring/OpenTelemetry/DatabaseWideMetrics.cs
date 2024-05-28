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
    public const int IgnoreIndex = 0;
    private readonly DatabasesLandlord _databaseLandlord;
    private readonly string _databaseName;
    private readonly SemaphoreSlim _locker = new(1, 1);
    private bool _attached;
    private readonly HashSet<string> _loadedIndexes = new(StringComparer.OrdinalIgnoreCase);

    //Meter instances are universally shared across all indexes, as identification of indexes or databases occurs through measurement tags.
    private static readonly Lazy<Meter> IndexesMeter = new(() => new Meter(Constants.IndexMeter, "1.0.0"));
    private static readonly Lazy<Meter> DatabasesStorageMeter = new(() => new Meter(Constants.DatabaseStorageMeter, "1.0.0"));

    public DatabaseWideMetrics(DatabasesLandlord databaseLandlord, string databaseName, MonitoringConfiguration.OpenTelemetryConfiguration configuration) :
        base(configuration)
    {
        _databaseLandlord = databaseLandlord;
        _databaseName = databaseName;
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
        if (Configuration.ExposedDatabases != null && Configuration.ExposedDatabases.Contains(_databaseName) == false)
            return;

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

                if (Configuration.Indexes == false)
                    return;

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
            observeValueFactory: () => new DatabaseCountOfIndexes(_databaseName, _databaseLandlord, IgnoreIndex),
            family: configuration => configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseDocumentsStorageAllocatedSize>(
            name: Constants.DatabaseWide.DatabaseDocumentsStorageAllocatedSize,
            observeValueFactory: () => new DatabaseDocumentsStorageAllocatedSize(_databaseName, _databaseLandlord, IgnoreIndex),
            family: configuration => configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseDocumentsStorageUsedSize>(
            name: Constants.DatabaseWide.DatabaseDocumentsStorageUsedSize,
            observeValueFactory: () => new DatabaseDocumentsStorageUsedSize(_databaseName, _databaseLandlord, IgnoreIndex),
            family: configuration => configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseIndexStorageAllocatedSize>(
            name: Constants.DatabaseWide.DatabaseIndexStorageAllocatedSize,
            observeValueFactory: () => new DatabaseIndexStorageAllocatedSize(_databaseName, _databaseLandlord, IgnoreIndex),
            family: configuration => configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseIndexStorageUsedSize>(
            name: Constants.DatabaseWide.DatabaseIndexStorageUsedSize,
            observeValueFactory: () => new DatabaseIndexStorageUsedSize(_databaseName, _databaseLandlord, IgnoreIndex),
            family: configuration => configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseTotalStorageSize>(
            name: Constants.DatabaseWide.DatabaseTotalStorageSize,
            observeValueFactory: () => new DatabaseTotalStorageSize(_databaseName, _databaseLandlord, IgnoreIndex),
            family: configuration => configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseStorageDiskRemainingSpace>(
            name: Constants.DatabaseWide.DatabaseStorageDiskRemainingSpace,
            observeValueFactory: () => new DatabaseStorageDiskRemainingSpace(_databaseName, _databaseLandlord, IgnoreIndex),
            family: configuration => configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<int, DatabaseStorageDiskIosReadOperations>(
            name: Constants.DatabaseWide.DatabaseStorageDiskIosReadOperations,
            observeValueFactory: () => new DatabaseStorageDiskIosReadOperations(_databaseName, _databaseLandlord, IgnoreIndex),
            family: configuration => configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<int, DatabaseStorageDiskIosWriteOperations>(
            name: Constants.DatabaseWide.DatabaseStorageDiskIosWriteOperations,
            observeValueFactory: () => new DatabaseStorageDiskIosWriteOperations(_databaseName, _databaseLandlord, IgnoreIndex),
            family: configuration => configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseStorageDiskReadThroughput>(
            name: Constants.DatabaseWide.DatabaseStorageDiskReadThroughput,
            observeValueFactory: () => new DatabaseStorageDiskReadThroughput(_databaseName, _databaseLandlord, IgnoreIndex),
            family: configuration => configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseStorageDiskWriteThroughput>(
            name: Constants.DatabaseWide.DatabaseStorageDiskWriteThroughput,
            observeValueFactory: () => new DatabaseStorageDiskWriteThroughput(_databaseName, _databaseLandlord, IgnoreIndex),
            family: configuration => configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );

        CreateObservableGaugeWithTags<long, DatabaseStorageDiskQueueLength>(
            name: Constants.DatabaseWide.DatabaseStorageDiskQueueLength,
            observeValueFactory: () => new DatabaseStorageDiskQueueLength(_databaseName, _databaseLandlord, IgnoreIndex),
            family: configuration => configuration.DatabaseInstruments,
            meter: DatabasesStorageMeter
        );
    }

    private void RegisterIndexInstruments(string indexName)
    {
        CreateObservableGaugeWithTags<byte, DatabaseIndexExists>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexExists,
            observeValueFactory: () => new DatabaseIndexExists(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<byte, DatabaseIndexPriority>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexPriority,
            observeValueFactory: () => new DatabaseIndexPriority(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<byte, DatabaseIndexState>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexState,
            observeValueFactory: () => new DatabaseIndexState(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<int, DatabaseIndexErrors>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexErrors,
            observeValueFactory: () => new DatabaseIndexErrors(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableUpDownCounterWithTags<long, DatabaseIndexLastQueryTime>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexLastQueryTime,
            observeValueFactory: () => new DatabaseIndexLastQueryTime(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableUpDownCounterWithTags<long, DatabaseIndexLastIndexingTime>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexLastIndexingTime,
            observeValueFactory: () => new DatabaseIndexLastIndexingTime(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableUpDownCounterWithTags<long, DatabaseIndexTimeSinceLastQuery>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexTimeSinceLastQuery,
            observeValueFactory: () => new DatabaseIndexTimeSinceLastQuery(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableUpDownCounterWithTags<long, DatabaseIndexTimeSinceLastIndexing>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexTimeSinceLastIndexing,
            observeValueFactory: () => new DatabaseIndexTimeSinceLastIndexing(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<byte, DatabaseIndexLockMode>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexLockMode,
            observeValueFactory: () => new DatabaseIndexLockMode(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<byte, DatabaseIndexIsInvalid>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexIsInvalid,
            observeValueFactory: () => new DatabaseIndexIsInvalid(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<byte, DatabaseIndexStatus>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexStatus,
            observeValueFactory: () => new DatabaseIndexStatus(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<int, DatabaseIndexMapsPerSec>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexMapsPerSec,
            observeValueFactory: () => new DatabaseIndexMapsPerSec(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<int, DatabaseIndexReducesPerSec>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexReducesPerSec,
            observeValueFactory: () => new DatabaseIndexReducesPerSec(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);

        CreateObservableGaugeWithTags<byte, DatabaseIndexType>(
            name: Constants.DatabaseWide.IndexWide.DatabaseIndexType,
            observeValueFactory: () => new DatabaseIndexType(_databaseName, indexName, _databaseLandlord, IgnoreIndex, IgnoreIndex),
            family: configuration => configuration.IndexInstruments,
            IndexesMeter);
    }
}
