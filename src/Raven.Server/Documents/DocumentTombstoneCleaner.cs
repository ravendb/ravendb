using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Lucene.Net.Util;
using Sparrow.Logging;

using Sparrow.Collections;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Server.Documents
{
    public class DocumentTombstoneCleaner : IDisposable
    {
        private static Logger _logger;

        private bool _disposed;

        private readonly object _locker = new object();

        private readonly DocumentDatabase _documentDatabase;

        private readonly ConcurrentSet<IDocumentTombstoneAware> _subscriptions = new ConcurrentSet<IDocumentTombstoneAware>();

        private Timer _timer;

        public DocumentTombstoneCleaner(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
            _logger = LoggingSource.Instance.GetLogger<DocumentTombstoneCleaner>(_documentDatabase.Name);
        }

        public void Initialize()
        {
            _timer = new Timer(ExecuteCleanup, null, TimeSpan.FromMinutes(1), _documentDatabase.Configuration.Tombstones.Interval.AsTimeSpan);
        }

        public void Subscribe(IDocumentTombstoneAware subscription)
        {
            _subscriptions.Add(subscription);
        }

        public void Unsubscribe(IDocumentTombstoneAware subscription)
        {
            _subscriptions.TryRemove(subscription);
        }

        internal void ExecuteCleanup(object state)
        {
            if (Monitor.TryEnter(_locker) == false)
                return;

            try
            {
                if (_disposed)
                    return;

                var tombstones = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
                var storageEnvironment = _documentDatabase.DocumentsStorage.Environment;
                if (storageEnvironment == null) // doc storage was disposed before us?
                    return;
                using (var tx = storageEnvironment.ReadTransaction())
                {
                    foreach (var tombstoneCollection in _documentDatabase
                        .DocumentsStorage
                        .GetTombstoneCollections(tx))
                    {
                        tombstones[tombstoneCollection] = long.MaxValue;
                    }
                }

                if (tombstones.Count == 0)
                    return;

                long minAllDocsEtag = long.MaxValue;

                foreach (var subscription in _subscriptions)
                {
                    foreach (var tombstone in subscription.GetLastProcessedDocumentTombstonesPerCollection())
                    {
                        if (tombstone.Key == Constants.Replication.AllDocumentsCollection)
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
                                return;

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
                    _logger.Info($"Failed to execute tombstone cleanup on {_documentDatabase.Name}",e);
            }
            finally
            {
                Monitor.Exit(_locker);
            }
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