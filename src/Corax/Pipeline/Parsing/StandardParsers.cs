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
            if (AdvInstructionSet.X86.IsSupportedSse)
                return VectorParsers.FindFirstNonAsciiSse(buffer) == buffer.Length;

            return ScalarParsers.FindFirstNonAscii(buffer) == buffer.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int FindFirstNonAscii(ReadOnlySpan<byte> buffer)
        {
            if (AdvInstructionSet.X86.IsSupportedSse)
                return VectorParsers.FindFirstNonAsciiSse(buffer);

            return ScalarParsers.FindFirstNonAscii(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CountCodePointsFromUtf8(ReadOnlySpan<byte> buffer)
        {
            if (AdvInstructionSet.X86.IsSupportedSse)
                return VectorParsers.CountCodePointsFromUtf8(buffer);

            return ScalarParsers.CountCodePointsFromUtf8(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Utf16LengthFromUtf8(ReadOnlySpan<byte> buffer)
        {
            if (AdvInstructionSet.X86.IsSupportedSse)
                return VectorParsers.Utf16LengthFromUtf8(buffer);

            return ScalarParsers.Utf16LengthFromUtf8(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CountWhitespacesAscii(ReadOnlySpan<byte> buffer)
        {
            if (AdvInstructionSet.X86.IsSupportedSse)
                return VectorParsers.CountWhitespacesAscii(buffer);

            return ScalarParsers.CountWhitespacesAscii(buffer);
        }
    }
}
