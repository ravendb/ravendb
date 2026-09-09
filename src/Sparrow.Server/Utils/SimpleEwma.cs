using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Sparrow.Server.Utils
{
    public static class SimpleEwma
    {
        // we are tracking the validity of the average over time, if there are no events, it should expire
        public const long DefaultValidityMs = 30_000;

        // some signals should never expire
        public const long NeverExpires = long.MaxValue;
    }

    /// <summary>
    /// Exponentially weighted moving average over any 8-byte number (long, double), seeded by
    /// the first sample. Torn reads are not possible. Safe for a single writer with any number of concurrent readers.
    /// A reading older than the validity window reads as zero and the next sample re-seeds the average.
    /// </summary>
    public struct SimpleEwma<T>(int smoothing, long validityMs = SimpleEwma.DefaultValidityMs) where T : unmanaged, INumber<T>
    {
        static SimpleEwma()
        {
            if (Unsafe.SizeOf<T>() != sizeof(long))
                throw new NotSupportedException($"{typeof(T)} is not an 8-byte number - use long or double");
        }

        private long _bits;
        private long _lastUpdateMs;

        private readonly bool IsExpired(long lastUpdateMs) =>
            validityMs != SimpleEwma.NeverExpires &&
            (lastUpdateMs == 0 || Environment.TickCount64 - lastUpdateMs > validityMs);

        public T Current
        {
            get
            {
                // the stamp is written after the value (release), and read before it (acquire), so a
                // fresh stamp guarantees the value is at least as fresh
                if (IsExpired(Volatile.Read(ref _lastUpdateMs)))
                    return default;

                return Unsafe.BitCast<long, T>(Volatile.Read(ref _bits));
            }
        }

        public void Update(T sample)
        {
            var current = Current;

            // it is fine to "lose" samples under load, as this is a moving average and we are not trying to be precise
            var next = T.IsZero(current) ? sample : current + (sample - current) / T.CreateChecked(smoothing);
            Volatile.Write(ref _bits, Unsafe.BitCast<T, long>(next));
            Volatile.Write(ref _lastUpdateMs, Environment.TickCount64);
        }
    }
}
