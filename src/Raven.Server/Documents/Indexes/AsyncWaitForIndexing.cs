using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Sparrow.Server;

namespace Raven.Server.Documents.Indexes
{
    public class AsyncWaitForIndexing
    {
        private readonly Stopwatch _queryDuration;
        private readonly TimeSpan _waitTimeout;
        private readonly Index _index;
        private readonly bool _isMaxTimeout;

        public AsyncWaitForIndexing(Stopwatch queryDuration, TimeSpan waitTimeout, Index index)
        {
            _queryDuration = queryDuration;
            _waitTimeout = waitTimeout;
            _isMaxTimeout = waitTimeout == TimeSpan.MaxValue;
            _index = index;
        }

        public bool TimeoutExceeded;

        public Task WaitForIndexingAsync(AsyncManualResetEvent.FrozenAwaiter indexingBatchCompleted)
        {
            _index.AssertNotDisposed();

            if (_isMaxTimeout)
                return indexingBatchCompleted.WaitAsync();

            var remainingTime = _waitTimeout - _queryDuration.Elapsed;

            if (remainingTime <= TimeSpan.Zero)
            {
                TimeoutExceeded = true;
                return Task.CompletedTask;
            }

            return indexingBatchCompleted.WaitAsync(remainingTime);
        }
    }
}
