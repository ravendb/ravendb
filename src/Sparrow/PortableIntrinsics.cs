using System.Runtime.CompilerServices;

#if NET7_0_OR_GREATER
using System.Runtime.Intrinsics.X86;
#endif
#if NET9_0_OR_GREATER
using System.Runtime.Intrinsics.Arm;
#endif

namespace Sparrow
{
    internal static class PortableIntrinsics
    {
        /// <summary>
        /// JIT-time constant: true when software prefetch is available on this platform.
        /// Use to guard scan-ahead or range-walk logic that is only worthwhile when
        /// we can actually issue prefetch instructions.
        /// </summary>
        public static bool CanPrefetch
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
#if NET9_0_OR_GREATER
                if (Sve.IsSupported)
                    return true;
#endif
#if NET7_0_OR_GREATER
                if (Sse.IsSupported)
                    return true;
#endif
                return false;
            }
        }

        /// <summary>
        /// Prefetch a single cache line for temporal read (L1).
        /// x86: SSE PREFETCHT0.  ARM SVE: PRFM PLDL1KEEP.  Otherwise: no-op.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void PrefetchRead(void* address)
        {
#if NET7_0_OR_GREATER
            if (Sse.IsSupported)
            {
                Sse.Prefetch0(address);
                return;
            }
#endif
#if NET9_0_OR_GREATER
            if (Sve.IsSupported)
            {
                Sve.Prefetch8Bit(Sve.CreateTrueMaskByte(), address, SvePrefetchType.LoadL1Temporal);
            }
#endif
        }

        /// <summary>
        /// Prefetch a contiguous memory range for temporal read at 512-byte intervals.
        /// The stride primes the hardware sequential prefetcher across page boundaries.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static unsafe void PrefetchRange(byte* address, int length)
        {
            if (CanPrefetch == false)
                return;

            for (byte* p = address, end = address + length; p < end; p += 512)
                PrefetchRead(p);
        }
    }
}
