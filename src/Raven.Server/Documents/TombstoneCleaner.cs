using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Util;
using Raven.Server.Background;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Logging;
using Raven.Server.NotificationCenter;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public sealed class TombstoneCleaner : BackgroundWorkBase
    {
        private readonly ITombstoneAware.TombstoneType[] _tombstoneTypes = new ITombstoneAware.TombstoneType[]
        {
            ITombstoneAware.TombstoneType.Documents,
            ITombstoneAware.TombstoneType.TimeSeries,
            ITombstoneAware.TombstoneType.Counters
        };

        private readonly SemaphoreSlim _subscriptionsLocker = new SemaphoreSlim(1, 1);

        private readonly DocumentDatabase _documentDatabase;
        private readonly int _numberOfTombstonesToDeleteInBatch;

        private readonly HashSet<ITombstoneAware> _subscriptions = new HashSet<ITombstoneAware>();
        private long? _maxTombstoneEtagToDelete;

        public TombstoneCleaner(DocumentDatabase documentDatabase) : base(documentDatabase.Name, RavenLogManager.Instance.GetLoggerForDatabase<TombstoneCleaner>(documentDatabase), documentDatabase.DatabaseShutdown)
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

        public IDisposable PreventTombstoneCleaningUpToEtag(long maxTombstoneToDelete)
        {
            _maxTombstoneEtagToDelete = maxTombstoneToDelete;

            return new DisposableAction(() =>
            {
                _maxTombstoneEtagToDelete = null;
            });
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

                var state = GetState();
                if (state.Tombstones.Count == 0)
                    return numberOfTombstonesDeleted;

                var batchSize = numberOfTombstonesToDeleteInBatch ?? _numberOfTombstonesToDeleteInBatch;

                while (CancellationToken.IsCancellationRequested == false)
                {
                    var command = new DeleteTombstonesCommand(state.Tombstones, state.MinAllDocsEtag, state.MinAllTimeSeriesEtag, state.MinAllCountersEtag, batchSize, _documentDatabase, Logger);
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

        private void RaiseBlockingTombstonesNotificationIfNecessary(TombstonesState tombstoneCollections)
        {
            var detailsSet = new List<BlockingTombstoneDetails>();
            var tombstonesCountsPerCollection = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            var tombstonesSizePerCollection = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

            using (_documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var disabledSubscribers in _subscriptions
                             .Select(x => x.GetDisabledSubscribersCollections(tombstoneCollections.Tombstones.Keys.ToHashSet())))
                {
                    FillDetailsSet(detailsSet, disabledSubscribers, tombstonesCountsPerCollection, tombstonesSizePerCollection, context);
                }
            }

            UpdateNotifications(detailsSet);
        }

        private void FillDetailsSet(
            List<BlockingTombstoneDetails> detailsSet,
            Dictionary<TombstoneDeletionBlockageSource, HashSet<string>> disabledSubscribers,
            IDictionary<string, long> tombstonesCountsPerCollection,
            IDictionary<string, long> tombstonesSizePerCollection,
            DocumentsOperationContext context)
        {
            foreach ((TombstoneDeletionBlockageSource source, HashSet<string> collections) in disabledSubscribers)
            {
                detailsSet.AddRange(
                    from collectionName in collections
                    let tombstonesCount = GetTombstoneDataForCollection(tombstonesCountsPerCollection, collectionName, context, _documentDatabase.DocumentsStorage.TombstonesCountForCollection)
                    let tombstonesSizeInBytes = GetTombstoneDataForCollection(tombstonesSizePerCollection, collectionName, context, _documentDatabase.DocumentsStorage.TombstonesSizeForCollectionInBytes)
                    where tombstonesCount > 0
                    select new BlockingTombstoneDetails
                    {
                        Source = source.Name,
                        BlockerType = source.Type,
                        BlockerTaskId = source.TaskId,
                        Collection = collectionName,
                        NumberOfTombstones = tombstonesCount,
                        SizeOfTombstonesInBytes = tombstonesSizeInBytes
                    });
            }
        }

        private static long GetTombstoneDataForCollection(
            IDictionary<string, long> dataPerCollection,
            string collectionName,
            DocumentsOperationContext context,
            Func<DocumentsOperationContext, string, long> retrieveDataFunc)
        {
            if (dataPerCollection.TryGetValue(collectionName, out var data))
                return data;

            data = retrieveDataFunc(context, collectionName);
            dataPerCollection[collectionName] = data;

            return data;
        }

        private void UpdateNotifications(List<BlockingTombstoneDetails> detailsSet)
        {
            if (detailsSet.Count > 0)
                _documentDatabase.NotificationCenter.TombstoneNotifications.Add(detailsSet);
            else
                _documentDatabase.NotificationCenter.Dismiss(AlertRaised.GetKey(AlertType.BlockingTombstones, nameof(AlertType.BlockingTombstones)));
        }

        internal TombstonesState GetState(bool addInfoForDebug = false)
        {
            var result = new TombstonesState();

            if (CancellationToken.IsCancellationRequested)
                return result;

            var storageEnvironment = _documentDatabase?.DocumentsStorage?.Environment;
            if (storageEnvironment == null) // doc storage was disposed before us?
                return result;

            using (var tx = storageEnvironment.ReadTransaction())
            {
                foreach (var tombstoneCollection in _documentDatabase.DocumentsStorage.GetTombstoneCollections(tx))
                {
                    result.Tombstones[tombstoneCollection] = new StateHolder();
                }
            }

            if (result.Tombstones.Count == 0)
                return result;

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

                        Debug.Assert(new[] { Constants.TimeSeries.All, Constants.Documents.Collections.AllDocumentsCollection, Constants.Counters.All }.All(x => subscriptionTombstones.Keys.Contains(x)) == false);

                        foreach (var tombstone in subscriptionTombstones)
                        {
                            if (addInfoForDebug)
                                result.AddPerSubscriptionInfo(subscription.TombstoneCleanerIdentifier, tombstoneType, collection: tombstone.Key, etag: tombstone.Value);

                            if (tombstone.Key == Constants.Documents.Collections.AllDocumentsCollection)
                            {
                                result.MinAllDocsEtag = Math.Min(tombstone.Value, result.MinAllDocsEtag);
                                break;
                            }

                            if (tombstone.Key == Constants.TimeSeries.All)
                            {
                                result.MinAllTimeSeriesEtag = Math.Min(tombstone.Value, result.MinAllTimeSeriesEtag);
                                break;
                            }

                            if (tombstone.Key == Constants.Counters.All)
                            {
                                result.MinAllCountersEtag = Math.Min(tombstone.Value, result.MinAllCountersEtag);
                                break;
                            }

                            var state = GetStateInternal(result.Tombstones, tombstone.Key, tombstoneType);
                            if (tombstone.Value < state.Etag)
                            {
                                state.Component = subscription.TombstoneCleanerIdentifier;
                                state.Etag = tombstone.Value;
                            }
                        }
                    }
                }

                try
                {
                    RaiseBlockingTombstonesNotificationIfNecessary(result);
                }
                catch (Exception e)
                {
                    if (Logger.IsWarnEnabled)
                        Logger.Warn($"Failed to notify of blockage in tombstone deletion detected in database '{_documentDatabase.Name}'", e);
                }
            }
            finally
            {
                _subscriptionsLocker.Release();
            }

            var maxTombstoneEtagToDelete = _maxTombstoneEtagToDelete;
            if (maxTombstoneEtagToDelete.HasValue)
            {
                result.MinAllDocsEtag = Math.Min(result.MinAllDocsEtag, maxTombstoneEtagToDelete.Value);
                result.MinAllCountersEtag = Math.Min(result.MinAllCountersEtag, maxTombstoneEtagToDelete.Value);
                result.MinAllTimeSeriesEtag = Math.Min(result.MinAllTimeSeriesEtag, maxTombstoneEtagToDelete.Value);
            }

            return result;

            static State GetStateInternal(Dictionary<string, StateHolder> results, string collection, ITombstoneAware.TombstoneType type)
            {
                if (results.TryGetValue(collection, out var value) == false)
                    results[collection] = value = new StateHolder();

                switch (type)
                {
                    case ITombstoneAware.TombstoneType.Documents:
                        return value.Documents;
                    case ITombstoneAware.TombstoneType.TimeSeries:
                        return value.TimeSeries;
                    case ITombstoneAware.TombstoneType.Counters:
                        return value.Counters;
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
                Counters = new State();
            }

            public State Documents;

            public State TimeSeries;

            public State Counters;
        }

        public sealed class State
        {
            public State()
            {
                Component = null;
                Etag = long.MaxValue;
            }

            public string Component;

            public long Etag;
        }

        internal class TombstonesState
        {
            public TombstonesState()
            {
                Tombstones = new Dictionary<string, StateHolder>(StringComparer.OrdinalIgnoreCase);
                MinAllDocsEtag = long.MaxValue;
                MinAllTimeSeriesEtag = long.MaxValue;
                MinAllCountersEtag = long.MaxValue;
            }

            public Dictionary<string, StateHolder> Tombstones { get; set; }

            public long MinAllDocsEtag { get; set; }

            public long MinAllTimeSeriesEtag { get; set; }

            public long MinAllCountersEtag { get; set; }

            public List<SubscriptionInfo> PerSubscriptionInfo;

            public void AddPerSubscriptionInfo(string identifier, ITombstoneAware.TombstoneType type, string collection, long etag)
            {
                PerSubscriptionInfo ??= new List<SubscriptionInfo>();
                PerSubscriptionInfo.Add(new SubscriptionInfo
                {
                    Identifier = identifier,
                    Type = type,
                    Collection = collection,
                    Etag = etag
                });
            }

            public sealed class SubscriptionInfo
            {
                public string Identifier { get; set; }

                public ITombstoneAware.TombstoneType Type { get; set; }

                public string Collection { get; set; }

                public long Etag { get; set; }
            }
        }

        internal class DeleteTombstonesCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
        {
            private readonly Dictionary<string, StateHolder> _tombstones;
            private readonly long _minAllDocsEtag;
            private readonly long _minAllTimeSeriesEtag;
            private readonly long _minAllCountersEtag;
            private readonly long _numberOfTombstonesToDeleteInBatch;
            private readonly DocumentDatabase _database;
            private readonly RavenLogger _logger;

            public long NumberOfTombstonesDeleted { get; private set; }

            public DeleteTombstonesCommand(Dictionary<string, StateHolder> tombstones, long minAllDocsEtag, long minAllTimeSeriesEtag, long minAllCountersEtag, long numberOfTombstonesToDeleteInBatch, DocumentDatabase database, RavenLogger logger)
            {
                _tombstones = tombstones ?? throw new ArgumentNullException(nameof(tombstones));
                _minAllDocsEtag = minAllDocsEtag;
                _minAllTimeSeriesEtag = minAllTimeSeriesEtag;
                _minAllCountersEtag = minAllCountersEtag;
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

                        var deletedCounterTombstoneEntries = ProcessCounters(context, tombstone.Value.Counters, tombstone.Key, numberOfTombstonesToDeleteInBatch);
                        numberOfTombstonesToDeleteInBatch -= deletedCounterTombstoneEntries;
                        NumberOfTombstonesDeleted += deletedCounterTombstoneEntries;

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

            public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
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

            private long ProcessCounters(DocumentsOperationContext context, State state, string collection, long numberOfTombstonesToDeleteInBatch)
            {
                if (state == null)
                    return 0;

                var minTombstoneValue = Math.Min(state.Etag, _minAllCountersEtag);
                if (minTombstoneValue <= 0)
                    return 0;

                return _database.DocumentsStorage.CountersStorage.PurgeCountersAndCounterTombstones(context, collection, minTombstoneValue, numberOfTombstonesToDeleteInBatch);
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

    internal class DeleteTombstonesCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, TombstoneCleaner.DeleteTombstonesCommand>
    {
        public Dictionary<string, TombstoneCleaner.StateHolder> Tombstones;
        public long MinAllDocsEtag;
        public long MinAllTimeSeriesEtag;
        public long MinAllCountersEtag;
        public long? NumberOfTombstonesToDeleteInBatch;

        public TombstoneCleaner.DeleteTombstonesCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var log = RavenLogManager.Instance.GetLoggerForDatabase<DeleteTombstonesCommandDto>(database);
            var command = new TombstoneCleaner.DeleteTombstonesCommand(Tombstones, MinAllDocsEtag, MinAllTimeSeriesEtag, MinAllCountersEtag, NumberOfTombstonesToDeleteInBatch ?? long.MaxValue, database, log);
            return command;
        }
    }

    public interface ITombstoneAware
    {
        string TombstoneCleanerIdentifier { get; }

        Dictionary<string, long> GetLastProcessedTombstonesPerCollection(TombstoneType type);

        Dictionary<TombstoneDeletionBlockageSource, HashSet<string>> GetDisabledSubscribersCollections(HashSet<string> tombstoneCollections);

        public enum TombstoneType
        {
            Documents,
            TimeSeries,
            Counters
        }

        public enum TombstoneDeletionBlockerType
        {
            ExternalReplication,
            InternalReplication,
            RavenEtl,
            SqlEtl,
            OlapEtl,
            ElasticSearchEtl,
            QueueEtl,
            Backup,
            PullReplicationAsHub,
            PullReplicationAsSink,
            Index
        }
    }
}
