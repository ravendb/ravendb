using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class DocumentTombstoneCleaner : IDisposable
    {
        private static Logger _logger;

        private bool _disposed;

        private readonly object _locker = new object();

        private readonly DocumentDatabase _documentDatabase;

        private readonly HashSet<IDocumentTombstoneAware> _subscriptions = new HashSet<IDocumentTombstoneAware>();

        private Timer _timer;

        public DocumentTombstoneCleaner(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
            _logger = LoggingSource.Instance.GetLogger<DocumentTombstoneCleaner>(_documentDatabase.Name);
        }

        public void Initialize()
        {
            _timer = new Timer(_ => ExecuteCleanup(), null, TimeSpan.FromMinutes(1), _documentDatabase.Configuration.Tombstones.Interval.AsTimeSpan);
        }

        public void Subscribe(IDocumentTombstoneAware subscription)
        {
            lock (_locker)
            {
                _subscriptions.Add(subscription);
            }
        }

        public void Unsubscribe(IDocumentTombstoneAware subscription)
        {
            lock (_locker)
            {
                _subscriptions.Remove(subscription);
            }
        }

        internal bool ExecuteCleanup()
        {
            if (Monitor.TryEnter(_locker) == false)
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

                foreach (var tombstone in tombstones)
                {
                    var minTombstoneValue = Math.Min(tombstone.Value, minAllDocsEtag);
                    if (minTombstoneValue <= 0)
                        continue;

                    try
                    {
                        using (var tx = storageEnvironment.WriteTransaction())
                        {
                            if (_documentDatabase.DatabaseShutdown.IsCancellationRequested)
                                return true;

                            _documentDatabase.DocumentsStorage.DeleteTombstonesBefore(tombstone.Key, minTombstoneValue, tx);

                            tx.Commit();
                        }
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info(
                                $"Could not delete tombstones for '{tombstone.Key}' collection and '{minTombstoneValue}' etag.",
                                e);
                    }
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Failed to execute tombstone cleanup on {_documentDatabase.Name}", e);
            }
            finally
            {
                Monitor.Exit(_locker);
            }
            return true;
        }

        public void Dispose()
        {
            lock (_locker) // so we are sure we aren't running concurrently with the timer
            {
                _disposed = true;
                _timer?.Dispose();
                _timer = null;
            }
        }
    }

    public interface IDocumentTombstoneAware
    {
        Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection();
    }
}