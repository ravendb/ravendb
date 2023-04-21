using System;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Corax.Pipeline.Parsing;

namespace Corax.Pipeline
{
    public struct LowerCaseTransformer : ITransformer
    {
        public bool RequiresBufferSpace => true;
        
        public void Dispose() { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Transform(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        {
            if (StandardParsers.IsAscii(source))
            {
                return StandardTransformers.ToLowercaseAscii(source, tokens, ref dest, ref destTokens);
            }

            return StandardTransformers.ToLowercase(source, tokens, ref dest, ref destTokens);
        }

        public int Transform(ReadOnlySpan<char> source, ReadOnlySpan<Token> tokens, ref Span<char> dest, ref Span<Token> destTokens)
        {
            source.ToLowerInvariant(dest);
            if (source != dest)
                tokens.CopyTo(destTokens);

            // We need to shrink the tokens and bytes output. 
            destTokens = destTokens.Slice(0, tokens.Length);
            dest = dest.Slice(0, source.Length);

            return source.Length;
        }
    }
}
