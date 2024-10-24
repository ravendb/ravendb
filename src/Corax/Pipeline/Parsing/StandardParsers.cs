using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using Sparrow;

namespace Corax.Pipeline.Parsing
{
    internal static class StandardParsers
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool IsAscii(ReadOnlySpan<byte> buffer)
        {
            if (AdvInstructionSet.IsAcceleratedVector128)
                return VectorParsers.FindFirstNonAscii(buffer) == buffer.Length;

            return ScalarParsers.FindFirstNonAscii(buffer) == buffer.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FindFirstNonAscii(ReadOnlySpan<byte> buffer)
        {
            if (AdvInstructionSet.IsAcceleratedVector128)
                return VectorParsers.FindFirstNonAscii(buffer);

            return ScalarParsers.FindFirstNonAscii(buffer);
        }
    }
}
