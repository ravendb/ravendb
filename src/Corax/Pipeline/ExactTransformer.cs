using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Pipeline
{
    public struct ExactTransformer : ITransformer
    {
        public void Dispose() { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Transform(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        {
            tokens.CopyTo(destTokens);
            destTokens = destTokens.Slice(0, tokens.Length);
            dest = dest.Slice(0, source.Length);

            return source.Length;
        }

        public int Transform(ReadOnlySpan<char> source, ReadOnlySpan<Token> tokens, ref Span<char> dest, ref Span<Token> destTokens)
        {
            tokens.CopyTo(destTokens);
            destTokens = destTokens.Slice(0, tokens.Length);
            dest = dest.Slice(0, source.Length);

            return source.Length;
        }
    }
}
