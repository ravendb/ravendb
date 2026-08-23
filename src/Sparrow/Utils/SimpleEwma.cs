using System.Threading;

namespace Sparrow.Utils
{
    /// <summary>
    /// Integer exponentially weighted moving average, seeded by the first sample.
    /// Safe for a single writer with any number of concurrent readers.
    /// </summary>
    public struct SimpleEwma(int smoothing)
    {
        private long _value; // we rely on this being atomic & non torn 

        public long Current => Volatile.Read(ref _value);

        public void Update(long sample)
        {
            var current = Volatile.Read(ref _value);
            // it is fine to "lose" samples under load, as this is a moving average and we are not trying to be precise
            Volatile.Write(ref _value, current == 0 ? sample : current + (sample - current) / smoothing);
        }
    }
}
