using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Corax.Pipeline
{
    public interface ITransformer
    {
        bool SupportUtf8 => false;
        bool RequiresTokenSpace => false;
        bool RequiresBufferSpace => false;

        /// <summary>
        /// The token space multiplier ensures that the caller would call the Transform method providing enough buffer space to avoid an exception. 
        /// </summary>
        float TokenSpaceMultiplier => 1;

        /// <summary>
        /// The buffer space multiplier ensures that the caller would call the Transform method providing enough buffer space to avoid an exception. 
        /// </summary>
        float BufferSpaceMultiplier => 1;

        int Transform(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens);

        int Transform(ReadOnlySpan<char> source, ReadOnlySpan<Token> tokens, ref Span<char> dest, ref Span<Token> destTokens);
    }
}
