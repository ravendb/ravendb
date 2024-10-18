using System;
using System.Runtime.CompilerServices;

namespace Corax.Pipeline
{
    public interface ITokenFilter
    {
        bool SupportUtf8 => false;

        bool Accept(ReadOnlySpan<byte> source, in Token token);
        bool Accept(ReadOnlySpan<char> source, in Token token);
    }

    public readonly struct FilteringTokenFilter<TFilter>(TFilter filter) : IFilter
        where TFilter : struct, ITokenFilter
    {
        public bool SupportUtf8 => filter.SupportUtf8;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Filter(ReadOnlySpan<byte> source, ref Span<Token> tokens)
        {
            if (!filter.SupportUtf8)
                throw new NotSupportedException("This filter does not support UTF8");

            int storeIdx = 0;
            for (int i = 0; i < tokens.Length; i++)
            {
                ref var token = ref tokens[i];

                if (filter.Accept(source.Slice(token.Offset, (int)token.Length), token))
                {
                    tokens[storeIdx] = token;
                    storeIdx++;
                }
            }

            tokens = tokens.Slice(0, storeIdx);
            return source.Length;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Filter(ReadOnlySpan<char> source, ref Span<Token> tokens)
        {
            int storeIdx = 0;
            for (int i = 0; i < tokens.Length; i++)
            {
                ref var token = ref tokens[i];

                if (filter.Accept(source.Slice(token.Offset, (int)token.Length), token))
                {
                    tokens[storeIdx] = token;
                    storeIdx++;
                }
            }

            tokens = tokens.Slice(0, storeIdx);
            return source.Length;
        }

        public void Dispose() {}
    }
}
