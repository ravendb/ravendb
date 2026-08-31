using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Sparrow.Server.Utils
{
    /// <summary>
    /// Exponentially weighted moving average over any 8-byte number (long, double), seeded by
    /// the first sample. Torn reads are not possible.
    /// Safe for a single writer with any number of concurrent readers.
    /// </summary>
    public struct SimpleEwma<T>(int smoothing) where T : unmanaged, INumber<T>
    {
        static SimpleEwma()
        {
            if (Unsafe.SizeOf<T>() != sizeof(long))
                throw new NotSupportedException($"{typeof(T)} is not an 8-byte number - use long or double");
        }

        private long _bits;

        public T Current => Unsafe.BitCast<long, T>(Volatile.Read(ref _bits));

        public void Update(T sample)
        {
            var current = Unsafe.BitCast<long, T>(Volatile.Read(ref _bits));
            // it is fine to "lose" samples under load, as this is a moving average and we are not trying to be precise
            var next = T.IsZero(current) ? sample : current + (sample - current) / T.CreateChecked(smoothing);
            Volatile.Write(ref _bits, Unsafe.BitCast<T, long>(next));
        }
    }
}
