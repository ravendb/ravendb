using System;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Corax.Pipeline.Parsing;
using Sparrow;

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
    }
    
    // This is a compatibility implementation for indexes that were created before RavenDB-24423. It should be used only with KeywordTokenizer.
    public struct LowerCaseTransformerPre24423 : ITransformer
    {
        public bool RequiresBufferSpace => true;
    
        public int Transform(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        {
            var charCount = (uint)Encodings.Utf8.GetCharCount(source.Slice(0, (int)tokens[0].Length));
            var result = ScalarTransformers.ToLowercasePre24423(source, tokens, ref dest, ref destTokens);
            destTokens[0].Length = AdjustTokenLengthPre24423(dest, charCount);
            return result;
        }
    
        /// <summary>
        /// Replicates the token adjustment behavior from the old RunUtf8WithConversion path in Analyzer.cs.
        /// </summary>
        private static uint AdjustTokenLengthPre24423(ReadOnlySpan<byte> output, uint charCount)
        {
            uint processedChars = 0;
            uint excessBytes = 0;
            int pos = 0;
    
            while (pos < output.Length)
            {
                byte b = output[pos];
                switch (b)
                {
                    case <= 0b0111_1111:
                        break;
                    case <= 0b1101_1111:
                        pos += 1;
                        excessBytes += 1;
                        break;
                    case <= 0b1110_1111:
                        pos += 2;
                        excessBytes += 2;
                        break;
                    default:
                        pos += 3;
                        excessBytes += 3;
                        break;
                }
    
                processedChars++;
                pos++;
    
                if (processedChars == charCount)
                    return charCount + excessBytes;
            }
            
            return charCount;
        }
    }
    
    
    // This is a compatibility implementation for indexes that were created with an ASCII only implementation. This implementation
    // MUST NOT be used for any other purpose than backward compatibility and MUST be removed on 7.0
    public struct LowerCaseTransformerPre22999 : ITransformer
    {
        public void Dispose() { }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Transform(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
        {
            for (int i = 0; i < source.Length; i++)
            {
                var value = source[i];
                if (value >= 'A' && value <= 'Z')
                {
                    dest[i] = (byte)(value + ('a' - 'A'));
                }
            }

            tokens.CopyTo(destTokens);

            // We need to shrink the tokens and bytes output. 
            destTokens = destTokens.Slice(0, tokens.Length);
            dest = dest.Slice(0, source.Length);

            return source.Length;
        }

        public int Transform(ReadOnlySpan<char> source, ReadOnlySpan<Token> tokens, ref Span<char> dest, ref Span<Token> destTokens)
        {
            for (int i = 0; i < source.Length; i++)
            {
                var value = source[i];
                if (value >= 'A' && value <= 'Z')
                {
                    dest[i] = (char)(value + ('a' - 'A'));
                }
            }

            tokens.CopyTo(destTokens);

            // We need to shrink the tokens and bytes output. 
            destTokens = destTokens.Slice(0, tokens.Length);
            dest = dest.Slice(0, source.Length);

            return source.Length;
        }
    }
}
