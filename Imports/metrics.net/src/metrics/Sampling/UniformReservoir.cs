
using System;
using System.Linq;
using Metrics.Utils;
namespace Metrics.Sampling
{
    public sealed class UniformReservoir : Reservoir
    {
        private const int DefaultSize = 1028;
        private const int BitsPerLong = 63;

        private AtomicLong count = new AtomicLong();

        private readonly long[] values;

        public UniformReservoir()
            : this(DefaultSize)
        { }

        public UniformReservoir(int size)
        {
            this.values = new long[size];
        }

        public long Count { get { return this.count.Value; } }

        public int Size
        {
            get
            {
                return Math.Min((int)this.count.Value, this.values.Length);
            }
        }

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
                count.SetValue(0L);
            }

            Array.Sort(values);
            return new UniformSnapshot(this.count.Value, values, valuesAreSorted: true);
        }

        public void Update(long value)
        {
            long c = this.count.Increment();
            if (c <= this.values.Length)
            {
                values[(int)c - 1] = value;
            }
            else
            {
                long r = NextLong(c);
                if (r < values.Length)
                {
                    values[(int)r] = value;
                }
            }
        }

        public void Reset()
        {
            count.SetValue(0L);
        }

        private static long NextLong(long max)
        {
            long bits, val;
            do
            {
                bits = ThreadLocalRandom.NextLong() & (~(1L << BitsPerLong));
                val = bits % max;
            } while (bits - val + (max - 1) < 0L);
            return val;
        }
    }
}
