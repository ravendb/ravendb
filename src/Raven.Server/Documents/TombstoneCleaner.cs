using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Background;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class TombstoneCleaner : BackgroundWorkBase
    {
        private readonly ITombstoneAware.TombstoneType[] _tombstoneTypes = new ITombstoneAware.TombstoneType[]
        {
            ITombstoneAware.TombstoneType.Documents,
            ITombstoneAware.TombstoneType.TimeSeries
        };

        private readonly SemaphoreSlim _subscriptionsLocker = new SemaphoreSlim(1, 1);

        private readonly DocumentDatabase _documentDatabase;
        private readonly int _numberOfTombstonesToDeleteInBatch;

        private readonly HashSet<ITombstoneAware> _subscriptions = new HashSet<ITombstoneAware>();

        public TombstoneCleaner(DocumentDatabase documentDatabase) : base(documentDatabase.Name, documentDatabase.DatabaseShutdown)
        {
            _documentDatabase = documentDatabase;
            _numberOfTombstonesToDeleteInBatch = _documentDatabase.Is32Bits
                ? 1024
                : 10 * 1024;
        }

        public void Subscribe(ITombstoneAware subscription)
        {
            _subscriptionsLocker.Wait();

            try
            {
                _subscriptions.Add(subscription);
            }
            finally
            {
                _subscriptionsLocker.Release();
            }
        }

        public void Unsubscribe(ITombstoneAware subscription)
        {
            _subscriptionsLocker.Wait();

            try
            {
                _subscriptions.Remove(subscription);
            }
            finally
            {
                _subscriptionsLocker.Release();
            }
        }

        protected override async Task DoWork()
        {
            await WaitOrThrowOperationCanceled(_documentDatabase.Configuration.Tombstones.CleanupInterval.AsTimeSpan);

            await ExecuteCleanup();
        }

        internal async Task<long> ExecuteCleanup(long? numberOfTombstonesToDeleteInBatch = null)
        {
            var numberOfTombstonesDeleted = 0L;

            try
            {
                if (CancellationToken.IsCancellationRequested)
                    return numberOfTombstonesDeleted;

                var state = GetStateInternal();
                if (state.Tombstones.Count == 0)
                    return numberOfTombstonesDeleted;

                var batchSize = numberOfTombstonesToDeleteInBatch ?? _numberOfTombstonesToDeleteInBatch;

                while (CancellationToken.IsCancellationRequested == false)
                {
                    var command = new DeleteTombstonesCommand(state.Tombstones, state.MinAllDocsEtag, state.MinAllTimeSeriesEtag, batchSize, _documentDatabase, Logger);
                    await _documentDatabase.TxMerger.Enqueue(command);

                    numberOfTombstonesDeleted += command.NumberOfTombstonesDeleted;

                    if (command.NumberOfTombstonesDeleted < batchSize)
                        break;
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to execute tombstone cleanup on {_documentDatabase.Name}", e);
            }

            return numberOfTombstonesDeleted;
        }

        internal Dictionary<string, StateHolder> GetState()
        {
            return GetStateInternal().Tombstones;
        }

        private (Dictionary<string, StateHolder> Tombstones, long MinAllDocsEtag, long MinAllTimeSeriesEtag) GetStateInternal()
        {
            var minAllDocsEtag = long.MaxValue;
            var minAllTimeSeriesEtag = long.MaxValue;
            var tombstones = new Dictionary<string, StateHolder>(StringComparer.OrdinalIgnoreCase);

            if (CancellationToken.IsCancellationRequested)
                return (tombstones, minAllDocsEtag, minAllTimeSeriesEtag);

            var storageEnvironment = _documentDatabase?.DocumentsStorage?.Environment;
            if (storageEnvironment == null) // doc storage was disposed before us?
                return (tombstones, minAllDocsEtag, minAllTimeSeriesEtag);

            using (var tx = storageEnvironment.ReadTransaction())
            {
                foreach (var tombstoneCollection in _documentDatabase.DocumentsStorage.GetTombstoneCollections(tx))
                    tombstones[tombstoneCollection] = new StateHolder();
            }

            if (tombstones.Count == 0)
                return (tombstones, minAllDocsEtag, minAllTimeSeriesEtag);

            _subscriptionsLocker.Wait();

            try
            {
                foreach (var subscription in _subscriptions)
                {
                    foreach (var tombstoneType in _tombstoneTypes)
                    {
                        var subscriptionTombstones = subscription.GetLastProcessedTombstonesPerCollection(tombstoneType);
                        if (subscriptionTombstones == null)
                            continue;

                        Debug.Assert(new[] { Constants.TimeSeries.All, Constants.Documents.Collections.AllDocumentsCollection }.All(x => subscriptionTombstones.Keys.Contains(x)) == false);

                        foreach (var tombstone in subscriptionTombstones)
                        {
                            if (tombstone.Key == Constants.Documents.Collections.AllDocumentsCollection)
                            {
                                minAllDocsEtag = Math.Min(tombstone.Value, minAllDocsEtag);
                                break;
                            }

                            if (tombstone.Key == Constants.TimeSeries.All)
                            {
                                minAllTimeSeriesEtag = Math.Min(tombstone.Value, minAllTimeSeriesEtag);
                                break;
                            }

                            var state = GetState(tombstones, tombstone.Key, tombstoneType);
                            if (tombstone.Value < state.Etag)
                            {
                                state.Component = subscription.TombstoneCleanerIdentifier;
                                state.Etag = tombstone.Value;
                            }
                        }
                    }
                }
            }
            finally
            {
                _subscriptionsLocker.Release();
            }

            return (tombstones, minAllDocsEtag, minAllTimeSeriesEtag);

            static State GetState(Dictionary<string, StateHolder> results, string collection, ITombstoneAware.TombstoneType type)
            {
                if (results.TryGetValue(collection, out var value) == false)
                    results[collection] = value = new StateHolder();

                switch (type)
                {
                    case ITombstoneAware.TombstoneType.Documents:
                        return value.Documents;
                    case ITombstoneAware.TombstoneType.TimeSeries:
                        return value.TimeSeries;
                    default:
                        throw new NotSupportedException($"Tombstone type '{type}' is not supported.");
                }
            }
        }

        internal class StateHolder
        {
            public StateHolder()
            {
                Documents = new State();
                TimeSeries = new State();
            }

            public State Documents;

            public State TimeSeries;
        }

        public class State
        {
            public State()
            {
                Component = null;
                Etag = long.MaxValue;
            }

            public string Component;

            public long Etag;
        }

        internal class DeleteTombstonesCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly Dictionary<string, StateHolder> _tombstones;
            private readonly long _minAllDocsEtag;
            private readonly long _minAllTimeSeriesEtag;
            private readonly long _numberOfTombstonesToDeleteInBatch;
            private readonly DocumentDatabase _database;
            private readonly Logger _logger;

            public long NumberOfTombstonesDeleted { get; private set; }

            public DeleteTombstonesCommand(Dictionary<string, StateHolder> tombstones, long minAllDocsEtag, long minAllTimeSeriesEtag, long numberOfTombstonesToDeleteInBatch, DocumentDatabase database, Logger logger)
            {
                _tombstones = tombstones ?? throw new ArgumentNullException(nameof(tombstones));
                _minAllDocsEtag = minAllDocsEtag;
                _minAllTimeSeriesEtag = minAllTimeSeriesEtag;
                _numberOfTombstonesToDeleteInBatch = numberOfTombstonesToDeleteInBatch;
                _database = database ?? throw new ArgumentNullException(nameof(database));
                _logger = logger ?? throw new ArgumentNullException(nameof(logger));
                UpdateAccessTime = false;
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                NumberOfTombstonesDeleted = 0;

                var numberOfTombstonesToDeleteInBatch = _numberOfTombstonesToDeleteInBatch;

                foreach (var tombstone in _tombstones)
                {
                    if (_database.DatabaseShutdown.IsCancellationRequested)
                        break;

                    try
                    {
                        var deletedSegmentsOrRanges = ProcessTimeSeries(context, tombstone.Value.TimeSeries, tombstone.Key, numberOfTombstonesToDeleteInBatch);
                        numberOfTombstonesToDeleteInBatch -= deletedSegmentsOrRanges;
                        NumberOfTombstonesDeleted += deletedSegmentsOrRanges;

                        if (numberOfTombstonesToDeleteInBatch <= 0)
                            break;

                        var numberOfEntriesDeleted = ProcessDocuments(context, tombstone.Value.Documents, tombstone.Key, numberOfTombstonesToDeleteInBatch);
                        numberOfTombstonesToDeleteInBatch -= numberOfEntriesDeleted;
                        NumberOfTombstonesDeleted += numberOfEntriesDeleted;

                        if (numberOfTombstonesToDeleteInBatch <= 0)
                            break;
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Could not delete tombstones for '{tombstone.Key}' collection before '{Math.Min(tombstone.Value.Documents.Etag, _minAllDocsEtag)}' etag for documents and '{Math.Min(tombstone.Value.TimeSeries.Etag, _minAllTimeSeriesEtag)}' etag for timeseries.", e);

                        throw;
                    }
                }

                return NumberOfTombstonesDeleted;
            }

            public override TransactionOperationsMerger.IReplayableCommandDto<TransactionOperationsMerger.MergedTransactionCommand> ToDto(JsonOperationContext context)
            {
                return new DeleteTombstonesCommandDto
                {
                    Tombstones = _tombstones,
                    MinAllDocsEtag = _minAllDocsEtag,
                    NumberOfTombstonesToDeleteInBatch = _numberOfTombstonesToDeleteInBatch
                };
            }

            private long ProcessTimeSeries(DocumentsOperationContext context, State state, string collection, long numberOfTombstonesToDeleteInBatch)
            {
                if (state == null)
                    return 0;

                var minTombstoneValue = Math.Min(state.Etag, _minAllTimeSeriesEtag);
                if (minTombstoneValue <= 0)
                    return 0;

                return _database.DocumentsStorage.TimeSeriesStorage.PurgeSegmentsAndDeletedRanges(context, collection, minTombstoneValue, numberOfTombstonesToDeleteInBatch);
            }

            private long ProcessDocuments(DocumentsOperationContext context, State state, string collection, long numberOfTombstonesToDeleteInBatch)
            {
                if (state == null)
                    return 0;

                var minTombstoneValue = Math.Min(state.Etag, _minAllDocsEtag);
                if (minTombstoneValue <= 0)
                    return 0;

                return _database.DocumentsStorage.DeleteTombstonesBefore(context, collection, minTombstoneValue, numberOfTombstonesToDeleteInBatch);
            }
        }
    }

    internal class DeleteTombstonesCommandDto : TransactionOperationsMerger.IReplayableCommandDto<TombstoneCleaner.DeleteTombstonesCommand>
    {
        public Dictionary<string, TombstoneCleaner.StateHolder> Tombstones;
        public long MinAllDocsEtag;
        public long MinAllTimeSeriesEtag;
        public long? NumberOfTombstonesToDeleteInBatch;

        public TombstoneCleaner.DeleteTombstonesCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var log = LoggingSource.Instance.GetLogger<TombstoneCleaner.DeleteTombstonesCommand>(database.Name);
            var command = new TombstoneCleaner.DeleteTombstonesCommand(Tombstones, MinAllDocsEtag, MinAllTimeSeriesEtag, NumberOfTombstonesToDeleteInBatch ?? long.MaxValue, database, log);
            return command;
        }
    }

    public interface ITombstoneAware
    {
        string TombstoneCleanerIdentifier { get; }

        Dictionary<string, long> GetLastProcessedTombstonesPerCollection(TombstoneType type);

        public enum TombstoneType
        {
            Documents,
            TimeSeries
        }
    }
}
