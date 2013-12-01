using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;

namespace Performance.Comparison
{
    public class PerfTracker
    {
        private ConcurrentQueue<long>  _durations = new ConcurrentQueue<long>();

        public void Record(long duration)
        {
            _durations.Enqueue(duration);
        }

        public IEnumerable<long> Checkout()
        {
            var concurrentQueue = _durations;
            _durations = new ConcurrentQueue<long>();
            return concurrentQueue;
        }
    }
}