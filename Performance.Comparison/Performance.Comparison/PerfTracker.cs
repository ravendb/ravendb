using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Performance.Comparison
{
    public class PerfTracker
    {
        private readonly PerformanceCounter _counter;

        private ConcurrentQueue<float> _values = new ConcurrentQueue<float>();

        public PerfTracker()
        {
            RecreateCategory();

            _counter = new PerformanceCounter("Voron Perf Test", "Ops/Sec", false);

            Task.Run(() =>
            {
                while (ShouldStop == false)
                {
                    Thread.Sleep(1000);
                    _values.Enqueue(_counter.NextValue());
                }
            });
        }

        public IEnumerable<float> Checkout()
        {
            var copy = _values;
            _values = new ConcurrentQueue<float>();
            return copy;
        }

        public bool ShouldStop { get; set; }

        public void Increment()
        {
            _counter.Increment();
        }

        private static void RecreateCategory()
        {
            if (PerformanceCounterCategory.Exists("Voron Perf Test"))
                PerformanceCounterCategory.Delete("Voron Perf Test");

            PerformanceCounterCategory.Create("Voron Perf Test", "none", PerformanceCounterCategoryType.SingleInstance,
                new CounterCreationDataCollection
                {
                    new CounterCreationData("Ops/Sec", "none", PerformanceCounterType.RateOfCountsPerSecond64)
                });
        }

    }
}