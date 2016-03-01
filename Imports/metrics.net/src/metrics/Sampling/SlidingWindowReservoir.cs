using System;
using System.Linq;
using Metrics.Utils;

namespace Metrics.Sampling
{
    public sealed class SlidingWindowReservoir : Reservoir
    {
        private const int DefaultSize = 1028;

        private readonly long[] values;
        private AtomicLong count = new AtomicLong();

        public SlidingWindowReservoir()
            : this(DefaultSize) { }

        public SlidingWindowReservoir(int size)
        {
            this.values = new long[size];
        }

        public void Update(long value)
        {
            var count = this.count.Increment();
            this.values[(int) ((count - 1)%values.Length)] = value;
        }

        public void Reset()
        {
            Array.Clear(this.values, 0, values.Length);
            count.SetValue(0L);
        }

        public long Count { get { return this.count.Value; } }
        public int Size { get { return Math.Min((int)this.count.Value, values.Length); } }

        public Snapshot GetSnapshot(bool resetReservoir = false)
        {
            var size = this.Size;
            if (size == 0)
            {
                return new UniformSnapshot(0, Enumerable.Empty<long>());
            }

            long[] values = new long[size];
            Array.Copy(this.values, values, size);

            if (resetReservoir)
            {
                Array.Clear(this.values, 0, values.Length);
                count.SetValue(0L);
            }

            Array.Sort(values);
            return new UniformSnapshot(this.count.Value, values, valuesAreSorted: true);
        }
    }
}
