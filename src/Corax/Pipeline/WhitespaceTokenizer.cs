using System;
using System.Runtime.CompilerServices;
using System.Text.Unicode;
using Corax.Pipeline.Parsing;

namespace Corax.Pipeline
{
    public struct WhitespaceTokenizer : ITokenizer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Tokenize(ReadOnlySpan<byte> source, ref Span<Token> tokens)
        {
            if (StandardParsers.IsAscii(source))
            {
                return StandardTokenizers.TokenizeWhitespaceAscii(source, ref tokens);
            }
            return StandardTokenizers.TokenizeWhitespace(source, ref tokens);
        }
        
        public void Dispose() { }
    }
}
