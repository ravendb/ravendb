using System;
using System.Collections.Generic;
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

        internal async Task ExecuteCleanup(long? numberOfTombstonesToDeleteInBatch = null)
        {
            try
            {
                if (CancellationToken.IsCancellationRequested)
                    return;

                var state = GetStateInternal();
                if (state.Tombstones.Count == 0)
                    return;

                var batchSize = numberOfTombstonesToDeleteInBatch ?? _numberOfTombstonesToDeleteInBatch;

                while (CancellationToken.IsCancellationRequested == false)
                {
                    var command = new DeleteTombstonesCommand(state.Tombstones, state.MinAllDocsEtag, batchSize, _documentDatabase, Logger);
                    await _documentDatabase.TxMerger.Enqueue(command);

                    if (command.NumberOfTombstonesDeleted < batchSize)
                        break;
                }
            }
            catch (Exception e)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info($"Failed to execute tombstone cleanup on {_documentDatabase.Name}", e);
            }
        }

        internal Dictionary<string, (string Component, long Value)> GetState()
        {
            return GetStateInternal().Tombstones;
        }

        private (Dictionary<string, (string Component, long Value)> Tombstones, long MinAllDocsEtag) GetStateInternal()
        {
            var minAllDocsEtag = long.MaxValue;
            var tombstones = new Dictionary<string, (string Component, long Value)>(StringComparer.OrdinalIgnoreCase);

            if (CancellationToken.IsCancellationRequested)
                return (tombstones, minAllDocsEtag);

            var storageEnvironment = _documentDatabase?.DocumentsStorage?.Environment;
            if (storageEnvironment == null) // doc storage was disposed before us?
                return (tombstones, minAllDocsEtag);

            using (var tx = storageEnvironment.ReadTransaction())
            {
                foreach (var tombstoneCollection in _documentDatabase.DocumentsStorage.GetTombstoneCollections(tx))
                {
                    tombstones[tombstoneCollection] = (null, long.MaxValue);
                }
            }

            if (tombstones.Count == 0)
                return (tombstones, minAllDocsEtag);

            _subscriptionsLocker.Wait();

            try
            {
                foreach (var subscription in _subscriptions)
                {
                    foreach (var tombstone in subscription.GetLastProcessedTombstonesPerCollection())
                    {
                        if (tombstone.Key == Constants.Documents.Collections.AllDocumentsCollection)
                        {
                            minAllDocsEtag = Math.Min(tombstone.Value, minAllDocsEtag);
                            break;
                        }

                        if (tombstones.TryGetValue(tombstone.Key, out var item) == false)
                            tombstones[tombstone.Key] = (subscription.TombstoneCleanerIdentifier, tombstone.Value);
                        else if (tombstone.Value < item.Value)
                            tombstones[tombstone.Key] = (subscription.TombstoneCleanerIdentifier, tombstone.Value);
                    }
                }
            }
            finally
            {
                _subscriptionsLocker.Release();
            }

            return (tombstones, minAllDocsEtag);
        }

        internal class DeleteTombstonesCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly Dictionary<string, (string Component, long Value)> _tombstones;
            private readonly long _minAllDocsEtag;
            private readonly long _numberOfTombstonesToDeleteInBatch;
            private readonly DocumentDatabase _database;
            private readonly Logger _logger;

            public long NumberOfTombstonesDeleted { get; private set; }

            public override bool UpdateAccessTime => false;

            public DeleteTombstonesCommand(Dictionary<string, (string Component, long Value)> tombstones, long minAllDocsEtag, long numberOfTombstonesToDeleteInBatch, DocumentDatabase database, Logger logger)
            {
                _tombstones = tombstones ?? throw new ArgumentNullException(nameof(tombstones));
                _minAllDocsEtag = minAllDocsEtag;
                _numberOfTombstonesToDeleteInBatch = numberOfTombstonesToDeleteInBatch;
                _database = database ?? throw new ArgumentNullException(nameof(database));
                _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            }

            protected override long ExecuteCmd(DocumentsOperationContext context)
            {
                NumberOfTombstonesDeleted = 0;

                var numberOfTombstonesToDeleteInBatch = _numberOfTombstonesToDeleteInBatch;

                foreach (var tombstone in _tombstones)
                {
                    var minTombstoneValue = Math.Min(tombstone.Value.Value, _minAllDocsEtag);
                    if (minTombstoneValue <= 0)
                        continue;

                    if (_database.DatabaseShutdown.IsCancellationRequested)
                        break;

                    try
                    {
                        var numberOfEntriesDeleted = _database.DocumentsStorage.DeleteTombstonesBefore(context, tombstone.Key, minTombstoneValue, numberOfTombstonesToDeleteInBatch);
                        numberOfTombstonesToDeleteInBatch -= numberOfEntriesDeleted;
                        NumberOfTombstonesDeleted += numberOfEntriesDeleted;
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info($"Could not delete tombstones for '{tombstone.Key}' collection before '{minTombstoneValue}' etag.", e);

                        throw;
                    }

                    if (numberOfTombstonesToDeleteInBatch <= 0)
                        break;
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
        }
    }

    internal class DeleteTombstonesCommandDto : TransactionOperationsMerger.IReplayableCommandDto<TombstoneCleaner.DeleteTombstonesCommand>
    {
        public Dictionary<string, (string Component, long Value)> Tombstones;
        public long MinAllDocsEtag;
        public long? NumberOfTombstonesToDeleteInBatch;

        public TombstoneCleaner.DeleteTombstonesCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
        {
            var log = LoggingSource.Instance.GetLogger<TombstoneCleaner.DeleteTombstonesCommand>(database.Name);
            var command = new TombstoneCleaner.DeleteTombstonesCommand(Tombstones, MinAllDocsEtag, NumberOfTombstonesToDeleteInBatch ?? long.MaxValue, database, log);
            return command;
        }
    }

    public interface ITombstoneAware
    {
        string TombstoneCleanerIdentifier { get; }

        Dictionary<string, long> GetLastProcessedTombstonesPerCollection();
    }
}
