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
                var tombstones = subscriptions
                .SelectMany(x => x.GetLastProcessedDocumentTombstonesPerCollection())
                .GroupBy(x => x.Key)
                .Select(x => new
                {
                    Collection = x.Key,
                    Etag = x.Min(y => y.Value)
                })
                .ToList();

                foreach (var tombstone in tombstones)
                {
                    if (_disposed)
                        return;

                    if (tombstone.Etag <= 0)
                        continue;

                    try
                    {
                        using (var tx = _documentDatabase.DocumentsStorage.Environment.WriteTransaction())
                        {
                            _documentDatabase.DocumentsStorage.DeleteTombstonesBefore(tombstone.Collection, tombstone.Etag, tx);

                            tx.Commit();
                        }
                    }
                    catch (Exception e)
                    {
                        Log.ErrorException($"Could not delete tombstones for '{tombstone.Collection}' collection and '{tombstone.Etag}' etag.", e);
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