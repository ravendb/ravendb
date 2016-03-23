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

        private bool _disposed;

        private readonly DocumentDatabase _documentDatabase;

        private readonly ConcurrentSet<IDocumentTombstoneAware> subscriptions = new ConcurrentSet<IDocumentTombstoneAware>();

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
            subscriptions.Add(subscription);
        }

        public void Unsubscribe(IDocumentTombstoneAware subscription)
        {
            subscriptions.TryRemove(subscription);
        }

        internal void ExecuteCleanup(object state)
        {
            if (Monitor.TryEnter(_locker) == false)
                return;

            try
            {
                Dictionary<string, long> tombstones;
                using (var tx = _documentDatabase.DocumentsStorage.Environment.ReadTransaction())
                {
                    tombstones = _documentDatabase
                        .DocumentsStorage
                        .GetTombstoneCollections(tx)
                        .ToDictionary(x => x, x => long.MaxValue, StringComparer.OrdinalIgnoreCase);
                }

                if (tombstones.Count == 0)
                    return;

                foreach (var subscription in subscriptions)
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
                    if (_disposed)
                        return;

                    if (tombstone.Value <= 0)
                        continue;

                    try
                    {
                        using (var tx = _documentDatabase.DocumentsStorage.Environment.WriteTransaction())
                        {
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
            _disposed = true;
            _timer?.Dispose();
        }
    }

    public interface IDocumentTombstoneAware
    {
        Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection();
    }
}