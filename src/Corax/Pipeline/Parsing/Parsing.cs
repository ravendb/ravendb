using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using NetTopologySuite.Operation.Buffer;


namespace Corax.Pipeline.Parsing
{
    public class Parsing
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool ValidateAscii(ReadOnlySpan<byte> buffer)
        {
            if (Sse41.IsSupported)
                return SseParsing.ValidateSse41Ascii(buffer);

            if (Sse2.IsSupported)
                return SseParsing.ValidateSse2Ascii(buffer);

            return ScalarParsing.ValidateAscii(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int CountCodePointsFromUtf8(ReadOnlySpan<byte> buffer)
        {
            if (Sse2.IsSupported)
                return SseParsing.CountCodePointsFromUtf8(buffer);

            return ScalarParsing.CountCodePointsFromUtf8(buffer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int Utf16LengthFromUtf8(ReadOnlySpan<byte> buffer)
        {
            if (Sse2.IsSupported)
                return SseParsing.Utf16LengthFromUtf8(buffer);

            return ScalarParsing.Utf16LengthFromUtf8(buffer);
        }
    }
}
