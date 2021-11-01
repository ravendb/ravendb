using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Corax.Pipeline
{
    public struct WhitespaceTokenizer : ITokenizer
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Tokenize(ReadOnlySpan<byte> source, ref Span<Token> tokens)
        {
            int i = 0;
            int currentToken = 0;

            while ( i < source.Length)
            {
                while (i < source.Length && source[i] == ' ')
                    i++;

                int start = i;
                while (i < source.Length && source[i] != ' ')
                    i++;

                if (start != i)
                {
                    ref var token = ref tokens[currentToken];
                    token.Offset = start;
                    token.Length = (uint)(i - start);
                    token.Type = TokenType.Word;

                    currentToken++;
                }
            }

            tokens = tokens.Slice(0, currentToken);
            return i;
        }

        public void Dispose() { }
    }
}
