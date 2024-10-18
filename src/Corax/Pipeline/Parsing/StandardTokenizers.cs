using System;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;
using Sparrow;

namespace Corax.Pipeline.Parsing
{
    internal static class StandardTokenizers
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TokenizeWhitespaceAscii(ReadOnlySpan<byte> buffer, ref Span<Token> tokens)
        {
            if (AdvInstructionSet.X86.IsSupportedSse)
                return VectorTokenizers.TokenizeWhitespaceAsciiSse(buffer, ref tokens);

            return ScalarTokenizers.TokenizeWhitespaceAsciiScalar(buffer, ref tokens);
        }

        public static int TokenizeWhitespace(ReadOnlySpan<byte> buffer, ref Span<Token> tokens)
        {
            if (StandardParsers.IsAscii(buffer))
            {
                return TokenizeWhitespaceAscii(buffer, ref tokens);
            }

            return ScalarTokenizers.TokenizeWhitespace(buffer, ref tokens);
        }
    }
}
