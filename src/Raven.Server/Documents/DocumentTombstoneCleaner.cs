using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Logging;

using Sparrow.Collections;

namespace Raven.Server.Documents
{
    public class DocumentTombstoneCleaner : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(DocumentTombstoneCleaner));

        private readonly object _locker = new object();

        private readonly DocumentDatabase _documentDatabase;

        private readonly ConcurrentSet<IDocumentTombstoneAware> _subscriptions = new ConcurrentSet<IDocumentTombstoneAware>();

        private Timer _timer;

        public DocumentTombstoneCleaner(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
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
                if (_timer == null)
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

                foreach (var subscription in _subscriptions)
                {
                    foreach (var tombstone in subscription.GetLastProcessedDocumentTombstonesPerCollection())
                    {
                        long v;
                        if (tombstones.TryGetValue(tombstone.Key, out v) == false)
                            tombstones[tombstone.Key] = tombstone.Value;
                        else
                            tombstones[tombstone.Key] = Math.Min(tombstone.Value, v);
                    }
                }

                foreach (var tombstone in tombstones)
                {
                    if (tombstone.Value <= 0)
                        continue;

                    try
                    {

                        using (var tx = storageEnvironment.WriteTransaction())
                        {
                            if (_documentDatabase.DatabaseShutdown.IsCancellationRequested)
                                return;

                            _documentDatabase.DocumentsStorage.DeleteTombstonesBefore(tombstone.Key, tombstone.Value, tx);

                            tx.Commit();
                        }
                    }
                    catch (Exception e)
                    {
                        Log.ErrorException($"Could not delete tombstones for '{tombstone.Key}' collection and '{tombstone.Value}' etag.", e);
                    }
                }
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