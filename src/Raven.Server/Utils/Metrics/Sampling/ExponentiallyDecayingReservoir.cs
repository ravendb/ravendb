using System;
using System.Collections.Generic;
using System.Threading;
using Metrics.Utils;
using Raven.Server.Utils.Metrics.Core;

namespace Metrics.Sampling
{
    public sealed class ExponentiallyDecayingReservoir : Reservoir,ITickable, IDisposable
    {
        private const int DefaultSize = 1028;
        private const double DefaultAlpha = 0.015;
        private static readonly TimeSpan RescaleInterval = TimeSpan.FromHours(1);

        private class ReverseOrderDoubleComparer : IComparer<double>
        {
            public static readonly IComparer<double> Instance = new ReverseOrderDoubleComparer();

            public int Compare(double x, double y)
            {
                return y.CompareTo(x);
            }
        }

        private readonly SortedList<double, WeightedSample> values;

        private SpinLock @lock = new SpinLock();

        private readonly double alpha;
        private readonly int size;
        private AtomicLong count = new AtomicLong();
        private AtomicLong startTime;

        private readonly ActionScheduler rescaleScheduler;

        public ExponentiallyDecayingReservoir(ActionScheduler scheduler, int size=0, double alpha=0 )
        {
            this.size = size==0?DefaultSize:size;
            this.alpha = alpha==0?DefaultAlpha:alpha;

            this.values = new SortedList<double, WeightedSample>(size, ReverseOrderDoubleComparer.Instance);

            this.rescaleScheduler = scheduler;
            this.rescaleScheduler.StartTickingMetric(RescaleInterval, this);

            this.startTime = new AtomicLong(Clock.Seconds);
        }

        public long Count { get { return this.count.Value; } }
        public int Size { get { return Math.Min(this.size, (int)this.count.Value); } }

        public Snapshot GetSnapshot(bool resetReservoir = false)
        {
            bool lockTaken = false;
            try
            {
                this.@lock.Enter(ref lockTaken);
                var snapshot = new WeightedSnapshot(this.count.Value, this.values.Values);
                if (resetReservoir)
                {
                    ResetReservoir();
                }
                return snapshot;
            }
            finally
            {
                if (lockTaken)
                {
                    this.@lock.Exit();
                }
            }
        }

        public void Update(long value)
        {
            Update(value, Clock.Seconds);
        }

        public void Reset()
        {
            bool lockTaken = false;
            try
            {
                this.@lock.Enter(ref lockTaken);
                ResetReservoir();
            }
            finally
            {
                if (lockTaken)
                {
                    this.@lock.Exit();
                }
            }
        }

        private void ResetReservoir()
        {
            this.values.Clear();
            this.count.SetValue(0L);
            this.startTime = new AtomicLong(Clock.Seconds);
        }

        private void Update(long value, long timestamp)
        {
            bool lockTaken = false;
            try
            {
                this.@lock.Enter(ref lockTaken);

                double itemWeight = Math.Exp(alpha * (timestamp - startTime.Value));
                var sample = new WeightedSample(value, itemWeight);

                double random = .0;
                // Prevent division by 0
                while (random.Equals(.0))
                {
                    random = ThreadLocalRandom.NextDouble();
                }

                double priority = itemWeight / random;

                long newCount = count.Increment();
                if (newCount <= size)
                {
                    this.values[priority] = sample;
                }
                else
                {
                    var first = this.values.Keys[this.values.Count - 1];
                    if (first < priority)
                    {
                        this.values.Remove(first);
                        this.values[priority] = sample;
                    }
                }
            }
            finally
            {
                if (lockTaken)
                {
                    this.@lock.Exit();
                }
            }
        }

        public void Dispose()
        {
            this.rescaleScheduler.StopTickingMetric(this);
        }



        ///* "A common feature of the above techniques—indeed, the key technique that
        // * allows us to track the decayed weights efficiently—is that they maintain
        // * counts and other quantities based on g(ti − L), and only scale by g(t − L)
        // * at query time. But while g(ti −L)/g(t−L) is guaranteed to lie between zero
        // * and one, the intermediate values of g(ti − L) could become very large. For
        // * polynomial functions, these values should not grow too large, and should be
        // * effectively represented in practice by floating point values without loss of
        // * precision. For exponential functions, these values could grow quite large as
        // * new values of (ti − L) become large, and potentially exceed the capacity of
        // * common floating point types. However, since the values stored by the
        // * algorithms are linear combinations of g values (scaled sums), they can be
        // * rescaled relative to a new landmark. That is, by the analysis of exponential
        // * decay in Section III-A, the choice of L does not affect the final result. We
        // * can therefore multiply each value based on L by a factor of exp(−α(L′ − L)),
        // * and obtain the correct value as if we had instead computed relative to a new
        // * landmark L′ (and then use this new L′ at query time). This can be done with
        // * a linear pass over whatever data structure is being used."
        // */
        private void Rescale()
        {
            bool lockTaken = false;
            try
            {
                this.@lock.Enter(ref lockTaken);
                long oldStartTime = startTime.Value;
                this.startTime.SetValue(Clock.Seconds);

                double scalingFactor = Math.Exp(-alpha * (startTime.Value - oldStartTime));

                var keys = new List<double>(this.values.Keys);
                foreach (var key in keys)
                {
                    WeightedSample sample = this.values[key];
                    this.values.Remove(key);
                    double newKey = key * Math.Exp(-alpha * (startTime.Value - oldStartTime));
                    var newSample = new WeightedSample(sample.Value, sample.Weight * scalingFactor);
                    this.values[newKey] = newSample;
                }
                // make sure the counter is in sync with the number of stored samples.
                this.count.SetValue(values.Count);
            }
            finally
            {
                if (lockTaken)
                {
                    this.@lock.Exit();
                }
            }
        }

        public void Tick()
        {
            Rescale();
        }
    }
}
