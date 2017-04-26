using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Background;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class DocumentTombstoneCleaner : BackgroundWorkBase
    {
        private static Logger _logger;

        private readonly SemaphoreSlim _locker = new SemaphoreSlim(1, 1);

        private readonly DocumentDatabase _documentDatabase;

        private readonly HashSet<IDocumentTombstoneAware> _subscriptions = new HashSet<IDocumentTombstoneAware>();

        public DocumentTombstoneCleaner(DocumentDatabase documentDatabase) : base(documentDatabase.Name, documentDatabase.DatabaseShutdown)
        {
            _documentDatabase = documentDatabase;
            _logger = LoggingSource.Instance.GetLogger<DocumentTombstoneCleaner>(_documentDatabase.Name);
        }

        public void Subscribe(IDocumentTombstoneAware subscription)
        {
            _locker.Wait();

            try
            {
                _subscriptions.Add(subscription);
            }
            finally
            {
                _locker.Release();
            }
        }

        public void Unsubscribe(IDocumentTombstoneAware subscription)
        {
            _locker.Wait();

            try
            {
                _subscriptions.Remove(subscription);
            }
            finally
            {
                _locker.Release();
            }
        }

        protected override async Task<bool> DoWork()
        {
            if (await WaitAsync(_documentDatabase.Configuration.Tombstones.Interval.AsTimeSpan) == false)
                return false;

            await ExecuteCleanup();

            return true;
        }

        internal async Task<bool> ExecuteCleanup()
        {
            if (await _locker.WaitAsync(0) == false)
                return false;

            try
            {
                if (CancellationToken.IsCancellationRequested)
                    return true;

                var tombstones = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
                var storageEnvironment = _documentDatabase.DocumentsStorage.Environment;
                if (storageEnvironment == null) // doc storage was disposed before us?
                    return true;

                using (var tx = storageEnvironment.ReadTransaction())
                {
                    foreach (var tombstoneCollection in _documentDatabase.DocumentsStorage.GetTombstoneCollections(tx))
                    {
                        tombstones[tombstoneCollection] = long.MaxValue;
                    }
                }

                if (tombstones.Count == 0)
                    return true;

                long minAllDocsEtag = long.MaxValue;

                foreach (var subscription in _subscriptions)
                {
                    foreach (var tombstone in subscription.GetLastProcessedDocumentTombstonesPerCollection())
                    {
                        if (tombstone.Key == Constants.Documents.Replication.AllDocumentsCollection)
                        {
                            minAllDocsEtag = Math.Min(tombstone.Value, minAllDocsEtag);
                            break;
                        }

                        long v;
                        if (tombstones.TryGetValue(tombstone.Key, out v) == false)
                            tombstones[tombstone.Key] = tombstone.Value;
                        else
                            tombstones[tombstone.Key] = Math.Min(tombstone.Value, v);
                    }
                }

                await _documentDatabase.TxMerger.Enqueue(new DeleteTombstonesCommand(tombstones, minAllDocsEtag, _documentDatabase));
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Failed to execute tombstone cleanup on {_documentDatabase.Name}", e);
            }
            finally
            {
                _locker.Release();
            }

            return true;
        }

        private class DeleteTombstonesCommand : TransactionOperationsMerger.MergedTransactionCommand
        {
            private readonly Dictionary<string, long> _tombstones;
            private readonly long _minAllDocsEtag;
            private readonly DocumentDatabase _database;

            public DeleteTombstonesCommand(Dictionary<string, long> tombstones, long minAllDocsEtag, DocumentDatabase database)
            {
                _tombstones = tombstones;
                _minAllDocsEtag = minAllDocsEtag;
                _database = database;
            }

            public override int Execute(DocumentsOperationContext context)
            {
                var deletionCount = 0;

                foreach (var tombstone in _tombstones)
                {
                    var minTombstoneValue = Math.Min(tombstone.Value, _minAllDocsEtag);
                    if (minTombstoneValue <= 0)
                        continue;

                    if (_database.DatabaseShutdown.IsCancellationRequested)
                        break;

                    deletionCount++;

                    try
                    {
                        _database.DocumentsStorage.DeleteTombstonesBefore(tombstone.Key, minTombstoneValue, context);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info(
                                $"Could not delete tombstones for '{tombstone.Key}' collection and '{minTombstoneValue}' etag.",
                                e);
                    }
                }

                return deletionCount;
            }
        }
    }

    public interface IDocumentTombstoneAware
    {
        Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection();
    }
}