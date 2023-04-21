using System;
using System.Runtime.CompilerServices;

namespace Corax.Pipeline
{
    public readonly struct FilterTransformer<TFilter>(TFilter filter) : ITransformer
        where TFilter : struct, ITokenFilter
    {        
        private readonly FilteringTokenFilter<TFilter> _filter = new(filter);

        public bool SupportUtf8 => _filter.SupportUtf8;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Transform(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        {
            // TODO: If they are the same input and output, dont do this. 
            tokens.CopyTo(destTokens);

            destTokens = destTokens.Slice(0, tokens.Length);

            return _filter.Filter(source, ref destTokens);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Transform(ReadOnlySpan<char> source, ReadOnlySpan<Token> tokens, ref Span<char> dest, ref Span<Token> destTokens)
        {
            // TODO: If they are the same input and output, dont do this. 
            tokens.CopyTo(destTokens);

            destTokens = destTokens.Slice(0, tokens.Length);

            return _filter.Filter(source, ref destTokens);
        }
    }
}
