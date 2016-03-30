using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Server.Documents.Indexes
{
    public class AsyncWaitForIndexing : IDisposable
    {
        private readonly string _indexName;
        private readonly Stopwatch _queryDuration;
        private readonly TimeSpan _waitTimeout;
        private readonly DocumentsNotifications _notifications;
        private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        public AsyncWaitForIndexing(string indexName, Stopwatch queryDuration, TimeSpan waitTimeout, DocumentsNotifications notifications)
        {
            _indexName = indexName;
            _queryDuration = queryDuration;
            _waitTimeout = waitTimeout;
            _notifications = notifications;
            _notifications.OnIndexChange += OnIndexChange;
        }

        public bool TimeoutExceeded;

        private void OnIndexChange(IndexChangeNotification notification)
        {
            if (string.Equals(notification.Name, _indexName, StringComparison.OrdinalIgnoreCase) == false)
                return;

            if (notification.Type == IndexChangeTypes.BatchCompleted)
                _semaphore.Release();
        }

        public Task WaitForIndexingAsync()
        {
            var remainingTime = _waitTimeout - _queryDuration.Elapsed;

            if (remainingTime <= TimeSpan.Zero)
            {
                TimeoutExceeded = true;
                return Task.CompletedTask;
            }

            return _semaphore.WaitAsync(remainingTime);
        }

        public void Dispose()
        {
            _semaphore.Dispose();
            _notifications.OnIndexChange -= OnIndexChange;
        }
    }
}