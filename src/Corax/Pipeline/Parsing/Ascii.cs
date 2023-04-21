using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;


namespace Corax.Pipeline.Parsing
{
    public class Ascii
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
    }
}
