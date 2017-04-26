using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class DocumentTombstoneCleaner : IDisposable
    {
        private static Logger _logger;

        private bool _disposed;

        private readonly SemaphoreSlim _locker = new SemaphoreSlim(1, 1);

        private readonly CancellationTokenSource _cts;

        private readonly DocumentDatabase _documentDatabase;

        private readonly HashSet<IDocumentTombstoneAware> _subscriptions = new HashSet<IDocumentTombstoneAware>();
        
        private Task _task;

        public DocumentTombstoneCleaner(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(documentDatabase.DatabaseShutdown);
            _logger = LoggingSource.Instance.GetLogger<DocumentTombstoneCleaner>(_documentDatabase.Name);
        }

        public void Initialize()
        {
            _task = Task.Run(Run, _cts.Token);
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

        private async Task Run()
        {
            while (_cts.IsCancellationRequested == false)
            {
                try
                {
                    try
                    {
                        await Task.Delay(_documentDatabase.Configuration.Tombstones.Interval.AsTimeSpan, _cts.Token);
                    }
                    catch (Exception)
                    {
                        // can happen if there is an invalid timespan
                        return;
                    }

                    await ExecuteCleanup();
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Error in the tombstone cleaner", e);
                }
            }
        }

        internal async Task<bool> ExecuteCleanup()
        {
            if (await _locker.WaitAsync(0) == false)
                return false;

            try
            {
                if (_disposed)
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

        public void Dispose()
        {
            _locker.Wait();

            try
            {
                _disposed = true;

                if (_task.Status == TaskStatus.Running)
                    _task.Wait();

                _cts.Dispose();
            }
            finally
            {
                _locker.Release();
            }
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