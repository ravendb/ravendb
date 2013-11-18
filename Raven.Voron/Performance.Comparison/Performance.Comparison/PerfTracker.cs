using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
            try
            {
                RecreateCategory();

                _counter = new PerformanceCounter("Voron Perf Test", "Ops/Sec", false);
            }
            catch (Exception e)
            {

            }
            Task.Run(() =>
            {
                while (ShouldStop == false && _counter != null)
                {
                    Thread.Sleep(1000);
                    _values.Enqueue(_counter.NextValue());
                }
            });
        }

        public IEnumerable<float> Checkout()
        {
            if (_counter == null)
                return Enumerable.Empty<float>();
            var copy = _values;
            _counter.RawValue = 0;
            _values = new ConcurrentQueue<float>();
            return copy;
        }

        public bool ShouldStop { get; set; }

        public void Increment()
        {
            if (_counter == null)
                return;
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