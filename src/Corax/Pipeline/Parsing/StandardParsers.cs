using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;

namespace Corax.Pipeline.Parsing
{
    public static class StandardParsers
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ValidateAscii(ReadOnlySpan<byte> buffer)
        {
            if (Sse41.IsSupported)
                return VectorParsers.ValidateSse41Ascii(buffer);

            if (Sse2.IsSupported)
                return VectorParsers.ValidateSse2Ascii(buffer);

            return ScalarParsers.ValidateAscii(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CountCodePointsFromUtf8(ReadOnlySpan<byte> buffer)
        {
            if (Sse2.IsSupported)
                return VectorParsers.CountCodePointsFromUtf8(buffer);

            return ScalarParsers.CountCodePointsFromUtf8(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Utf16LengthFromUtf8(ReadOnlySpan<byte> buffer)
        {
            if (Sse2.IsSupported)
                return VectorParsers.Utf16LengthFromUtf8(buffer);

            return ScalarParsers.Utf16LengthFromUtf8(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CountWhitespacesAscii(ReadOnlySpan<byte> buffer)
        {
            if (Sse2.IsSupported)
                return VectorParsers.CountWhitespacesAscii(buffer);

            return ScalarParsers.CountWhitespacesAscii(buffer);
        }
    }
}
