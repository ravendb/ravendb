using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;

namespace Corax.Pipeline.Parsing
{
    public static class StandardTokenizers
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TokenizeWhitespaceAscii(ReadOnlySpan<byte> buffer, ref Span<Token> tokens)
        {
            if (Sse2.IsSupported)
                return VectorTokenizers.TokenizeWhitespaceAsciiSse(buffer, ref tokens);

            return ScalarTokenizers.TokenizeWhitespaceAsciiScalar(buffer, ref tokens);
        }

        public static int TokenizeWhitespace(ReadOnlySpan<byte> buffer, ref Span<Token> tokens)
        {
            if (ScalarParsers.ValidateAscii(buffer))
            {
                return TokenizeWhitespace(buffer, ref tokens);
            }

            return ScalarTokenizers.TokenizeWhitespace(buffer, ref tokens);
        }

        public static int TokenizeWhitespace(ReadOnlySpan<char> buffer, ref Span<Token> tokens)
        {
            return ScalarTokenizers.TokenizeWhitespace(buffer, ref tokens);
        }
    }
}
