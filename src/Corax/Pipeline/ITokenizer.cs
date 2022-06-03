using System;

namespace Corax.Pipeline
{
    public interface ITokenizer : IDisposable
    {
        bool SupportUtf8 => false;

        /// <summary>
        /// Tokenize will take the source data and create the tokens from that source data.
        /// </summary>
        /// <returns>How many bytes from the source were consumed. This is just in case we need to eventually do multistep tokenization for example on attachments.</returns>
        int Tokenize(ReadOnlySpan<char> source, ref Span<Token> tokens);
        
        int Tokenize(ReadOnlySpan<byte> source, ref Span<Token> tokens);
    }
}
