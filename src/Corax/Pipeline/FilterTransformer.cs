using System;
using System.Runtime.CompilerServices;

namespace Corax.Pipeline
{
    public struct FilterTransformer<TFilter> : ITransformer
        where TFilter : struct, ITokenFilter
    {
        private readonly FilteringTokenFilter<TFilter> _filter;

        public FilterTransformer(TFilter filter)
        {
            _filter = new FilteringTokenFilter<TFilter>(filter);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Transform(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        {
            // TODO: If they are the same input and output, dont do this. 
            tokens.CopyTo(destTokens);

            destTokens = destTokens.Slice(0, tokens.Length);

            return _filter.Filter(source, ref destTokens);
        }
    }
}
