using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;

namespace Corax.Pipeline.Parsing
{
    internal static class StandardTokenizers
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int TokenizeWhitespaceAscii(ReadOnlySpan<byte> buffer, ref Span<Token> tokens)
        {
            Debug.Assert(buffer.Length <= tokens.Length);

            if (AdvInstructionSet.IsAcceleratedVector128)
                return VectorTokenizers.TokenizeWhitespaceAscii(buffer, ref tokens);

            return ScalarTokenizers.TokenizeWhitespaceAsciiScalar(buffer, ref tokens);
        }

        public static int TokenizeWhitespace(ReadOnlySpan<byte> buffer, ref Span<Token> tokens)
        {
            Debug.Assert(buffer.Length <= tokens.Length);
            
            if (StandardParsers.IsAscii(buffer))
            {
                return TokenizeWhitespaceAscii(buffer, ref tokens);
            }

            return ScalarTokenizers.TokenizeWhitespace(buffer, ref tokens);
        }
    }
}
