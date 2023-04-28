using System;
using System.Runtime.CompilerServices;
using Corax.Pipeline.Parsing;

namespace Corax.Pipeline
{
    public struct WhitespaceTokenizer : ITokenizer
    {
        public bool SupportUtf8 => true;


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Tokenize(ReadOnlySpan<byte> source, ref Span<Token> tokens)
        {
            if (StandardParsers.ValidateAscii(source))
            {
                return StandardTokenizers.TokenizeWhitespaceAscii(source, ref tokens);
            }
            return StandardTokenizers.TokenizeWhitespace(source, ref tokens);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Tokenize(ReadOnlySpan<char> source, ref Span<Token> tokens)
        {
            return ScalarTokenizers.TokenizeWhitespace(source, ref tokens);
        }

        public void Dispose() { }
    }
}
