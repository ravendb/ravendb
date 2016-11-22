using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Sparrow;

namespace Raven.Server.Documents.Indexes
{
    public class AsyncWaitForIndexing
    {
        private readonly Stopwatch _queryDuration;
        private readonly TimeSpan _waitTimeout;
        
        public AsyncWaitForIndexing(Stopwatch queryDuration, TimeSpan waitTimeout)
        {
            _queryDuration = queryDuration;
            _waitTimeout = waitTimeout;
        }

        public bool TimeoutExceeded;

        public Task WaitForIndexingAsync(AsyncManualResetEvent.FrozenAwaiter indexingBatchCompleted)
        {
            var remainingTime = _waitTimeout - _queryDuration.Elapsed;

            if (remainingTime <= TimeSpan.Zero)
            {
                TimeoutExceeded = true;
                return Task.CompletedTask;
            }

            var waitForIndexingAsync = indexingBatchCompleted.WaitAsync(TimeSpan.FromSeconds(15));
            return waitForIndexingAsync;
        }
    }
}