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
        private readonly AsyncManualResetEvent _indexingBatchCompleted;

        public AsyncWaitForIndexing(Stopwatch queryDuration, TimeSpan waitTimeout, AsyncManualResetEvent indexingBatchCompleted)
        {
            _queryDuration = queryDuration;
            _waitTimeout = waitTimeout;
            _indexingBatchCompleted = indexingBatchCompleted;
        }

        public bool TimeoutExceeded;

        public Task WaitForIndexingAsync()
        {
            var remainingTime = _waitTimeout - _queryDuration.Elapsed;

            if (remainingTime <= TimeSpan.Zero)
            {
                TimeoutExceeded = true;
                return Task.CompletedTask;
            }

            return _indexingBatchCompleted.WaitAsync(remainingTime);
        }
    }
}