using System;
using System.Buffers;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Corax.Pipeline;
using Sparrow.Server;

namespace Corax
{
    public unsafe class Analyzer : IDisposable
    {
        public static Analyzer CreateDefaultAnalyzer(ByteStringContext context) => Create(context, default(KeywordTokenizer), default(ExactTransformer));
        public static Analyzer CreateLowercaseAnalyzer(ByteStringContext context) => Create(context, default(KeywordTokenizer), default(LowerCaseTransformer));

        public static readonly ArrayPool<byte> BufferPool = ArrayPool<byte>.Create();
        public static readonly ArrayPool<Token> TokensPool = ArrayPool<Token>.Create();
        public readonly int DefaultOutputSize;
        public readonly int DefaultTokenSize;
        
        private readonly delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, ref byte[], void> _funcUtf8;
        private readonly delegate*<Analyzer, ReadOnlySpan<char>, ref Span<char>, ref Span<Token>, void> _funcUtf16;
        private readonly Analyzer _inner;
        private readonly ITokenizer _tokenizer;
        private readonly ITransformer[] _transformers;
        private readonly float _sourceBufferMultiplier;
        private readonly float _tokenBufferMultiplier;
        private bool _disposedValue;

        protected Analyzer(
            delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, ref byte[], void> functionUtf8,
            delegate*<Analyzer, ReadOnlySpan<char>, ref Span<char>, ref Span<Token>, void> functionUtf16,
            in ITokenizer tokenizer, ITransformer[] transformers) : this(null, functionUtf8, functionUtf16, tokenizer, transformers)
        {
        }

        protected Analyzer(Analyzer inner,
            delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, ref byte[], void> functionUtf8,
            delegate*<Analyzer, ReadOnlySpan<char>, ref Span<char>, ref Span<Token>, void> functionUtf16,
            in ITokenizer tokenizer, ITransformer[] transformers)
        {
            _inner = inner;
            _funcUtf8 = functionUtf8;
            _funcUtf16 = functionUtf16;
            _tokenizer = tokenizer;
            _transformers = transformers;
            
            float sourceBufferMultiplier = 1;
            float tokenBufferMultiplier = 1;
            foreach( var transformer in transformers)
            {
                if (transformer.BufferSpaceMultiplier > 1)
                    sourceBufferMultiplier *= transformer.BufferSpaceMultiplier;
                if (transformer.TokenSpaceMultiplier > 1)
                    tokenBufferMultiplier *= transformer.TokenSpaceMultiplier;
            }

            // If we have an inner analyzer block, we need to account for it's own multiplier effect. 
            if (inner != null)
            {
                sourceBufferMultiplier *= inner._sourceBufferMultiplier;
                tokenBufferMultiplier *= inner._tokenBufferMultiplier;
            }

            _sourceBufferMultiplier = sourceBufferMultiplier;
            _tokenBufferMultiplier = tokenBufferMultiplier;
            
            GetOutputBuffersSize(Constants.Analyzers.DefaultBufferForAnalyzers, out DefaultOutputSize, out DefaultTokenSize);
        }

        public void GetOutputBuffersSize(int inputSize, out int outputSize, out int tokenSize )
        {
            float bufferSize = _sourceBufferMultiplier * inputSize;
            outputSize = (int)bufferSize;
            if (bufferSize > outputSize)
                outputSize += 1;

            bufferSize = _tokenBufferMultiplier * inputSize;
            tokenSize = (int)bufferSize;
            if (bufferSize > tokenSize)
                tokenSize += 1;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int RunTransformer<TTransformer>(in TTransformer transformer, ReadOnlySpan<char> source, ReadOnlySpan<Token> tokens, ref Span<char> output, ref Span<Token> outputTokens)
            where TTransformer : ITransformer
        {
            byte[] sourceTempBufferHolder = null;
            Token[] tokensTempBufferHolder = null;

            Span<char> outputBufferSpace = output;
            if (transformer.RequiresBufferSpace)
            {
                sourceTempBufferHolder = BufferPool.Rent(source.Length * sizeof(char));
                outputBufferSpace = MemoryMarshal.Cast<byte, char>(sourceTempBufferHolder.AsSpan());
            }

            Span<Token> outputTokenSpace = outputTokens;
            if (transformer.RequiresTokenSpace)
            {
                tokensTempBufferHolder = TokensPool.Rent(tokens.Length);
                outputTokenSpace = tokensTempBufferHolder.AsSpan();
            }

            int result = transformer.Transform(source, tokens, ref outputBufferSpace, ref outputTokenSpace);
            output = output.Slice(0, outputBufferSpace.Length);
            outputTokens = outputTokens.Slice(0, outputTokenSpace.Length);

            if (transformer.RequiresBufferSpace)
            {
                outputBufferSpace.CopyTo(output);                
                BufferPool.Return(sourceTempBufferHolder);
            }

            if (transformer.RequiresTokenSpace)
            {
                outputTokenSpace.CopyTo(outputTokens);
                TokensPool.Return(tokensTempBufferHolder);
            }                      

            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int RunTransformer<TTransformer> (in TTransformer transformer, ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> output, ref Span<Token> outputTokens)
            where TTransformer : ITransformer
        {
            byte[] sourceTempBufferHolder = null;
            Token[] tokensTempBufferHolder = null;

            Span<byte> outputBufferSpace = output;
            if (transformer.RequiresBufferSpace)
            {
                sourceTempBufferHolder = BufferPool.Rent(source.Length);
                outputBufferSpace = sourceTempBufferHolder.AsSpan();
            }

            Span<Token> outputTokenSpace = outputTokens;
            if (transformer.RequiresTokenSpace)
            {
                tokensTempBufferHolder = TokensPool.Rent(tokens.Length);
                outputTokenSpace = tokensTempBufferHolder.AsSpan();
            }

            int result = transformer.Transform(source, tokens, ref outputBufferSpace, ref outputTokenSpace);
            output = output.Slice(0, outputBufferSpace.Length);
            outputTokens = outputTokens.Slice(0, outputTokenSpace.Length);

            if (transformer.RequiresBufferSpace)
            {
                outputBufferSpace.CopyTo(output);                
                BufferPool.Return(sourceTempBufferHolder);
            }

            if (transformer.RequiresTokenSpace)
            {
                outputTokenSpace.CopyTo(outputTokens);
                TokensPool.Return(tokensTempBufferHolder);
            }

            return result;
        }

        public struct NullTransformer : ITransformer
        {
            public bool SupportUtf8 => true;

            public int Transform(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
            {
                throw new InvalidOperationException();
            }

            public int Transform(ReadOnlySpan<char> source, ReadOnlySpan<Token> tokens, ref Span<char> dest, ref Span<Token> destTokens)
            {
                throw new InvalidOperationException();
            }
        }

        public struct NullTokenizer : ITokenizer
        {
            public bool SupportUtf8 => true;

            public void Dispose()
            {
            }

            public int Tokenize(ReadOnlySpan<byte> source, ref Span<Token> tokens)
            {
                throw new InvalidOperationException();
            }

            public int Tokenize(ReadOnlySpan<char> source, ref Span<Token> tokens)
            {
                throw new InvalidOperationException();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void RunUtf8<TTokenizer, TTransform1, TTransform2, TTransform3>(Analyzer analyzer,
                        ReadOnlySpan<byte> source, ref Span<byte> output, ref Span<Token> tokens, ref byte[] buffer)
            where TTokenizer : ITokenizer
            where TTransform1 : ITransformer
            where TTransform2 : ITransformer
            where TTransform3 : ITransformer
        {
            // We copy the span to have a local copy without modification. 
            var outputCopy = output;
            var outputTokensCopy = tokens;

            if (typeof(TTokenizer) != typeof(NullTokenizer))
            {
                int consumedBytes = ((TTokenizer)analyzer._tokenizer).Tokenize(source, ref tokens);

                source.CopyTo(output);
                output = output.Slice(0, consumedBytes);
            }
            else
            {
                analyzer._inner.Execute(source, ref output, ref tokens);
            }

            if (typeof(TTransform1) == typeof(NullTransformer))
                return;

            var destOutput = outputCopy;
            var destOutputTokens = outputTokensCopy;
            RunTransformer((TTransform1)analyzer._transformers[0], output, tokens, ref destOutput, ref destOutputTokens);
            output = destOutput;
            tokens = destOutputTokens;

            if (typeof(TTransform2) == typeof(NullTransformer))
                return;

            destOutput = outputCopy;
            destOutputTokens = outputTokensCopy;
            RunTransformer((TTransform2)analyzer._transformers[1], output, tokens, ref destOutput, ref destOutputTokens);
            output = destOutput;
            tokens = destOutputTokens;

            if (typeof(TTransform3) == typeof(NullTransformer))
                return;

            destOutput = outputCopy;
            destOutputTokens = outputTokensCopy;
            RunTransformer((TTransform3)analyzer._transformers[2], output, tokens, ref destOutput, ref destOutputTokens);
            output = destOutput;
            tokens = destOutputTokens;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void RunUtf16<TTokenizer, TTransform1, TTransform2, TTransform3>(Analyzer analyzer,
                ReadOnlySpan<char> source, ref Span<char> output, ref Span<Token> tokens)
            where TTokenizer : ITokenizer
            where TTransform1 : ITransformer
            where TTransform2 : ITransformer
            where TTransform3 : ITransformer
        {
            // We copy the span to have a local copy without modification. 
            var outputCopy = output;
            var outputTokensCopy = tokens;

            if (typeof(TTokenizer) != typeof(NullTokenizer))
            {
                int consumedBytes = ((TTokenizer)analyzer._tokenizer).Tokenize(source, ref tokens);

                source.CopyTo(output);
                output = output.Slice(0, consumedBytes);
            }
            else
            {
                analyzer._inner.Execute(source, ref output, ref tokens);
            }

            if (typeof(TTransform1) == typeof(NullTransformer))
                return;

            var destOutput = outputCopy;
            var destOutputTokens = outputTokensCopy;
            RunTransformer((TTransform1)analyzer._transformers[0], output, tokens, ref destOutput, ref destOutputTokens);
            output = destOutput;
            tokens = destOutputTokens;

            if (typeof(TTransform2) == typeof(NullTransformer))
                return;

            destOutput = outputCopy;
            destOutputTokens = outputTokensCopy;
            RunTransformer((TTransform2)analyzer._transformers[1], output, tokens, ref destOutput, ref destOutputTokens);
            output = destOutput;
            tokens = destOutputTokens;

            if (typeof(TTransform3) == typeof(NullTransformer))
                return;

            destOutput = outputCopy;
            destOutputTokens = outputTokensCopy;
            RunTransformer((TTransform3)analyzer._transformers[2], output, tokens, ref destOutput, ref destOutputTokens);
            output = destOutput;
            tokens = destOutputTokens;

        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Analyzer Create<TTokenizer, TTransform1, TTransform2, TTransform3>(ByteStringContext context, 
                            in TTokenizer tokenizer = default(TTokenizer), 
                            in TTransform1 transform1 = default(TTransform1),
                            in TTransform2 transform2 = default(TTransform2), 
                            in TTransform3 transform3 = default(TTransform3))
            where TTokenizer : ITokenizer
            where TTransform1 : ITransformer
            where TTransform2 : ITransformer
            where TTransform3 : ITransformer
        {
            delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, ref byte[], void> funcUtf8 = &RunUtf8<TTokenizer, TTransform1, TTransform2, TTransform3>;
            delegate*<Analyzer, ReadOnlySpan<char>, ref Span<char>, ref Span<Token>, void> funcUtf16 = &RunUtf16<TTokenizer, TTransform1, TTransform2, TTransform3>;

            static void RunUtf8WithConversion(Analyzer analyzer, ReadOnlySpan<byte> source, ref Span<byte> output, ref Span<Token> tokens, ref byte[] buffer)
            {
                if (buffer.Length < source.Length * 10)
                {
                    if (buffer.Length > 0)
                        BufferPool.Return(buffer);
                    buffer = null;
                    buffer = BufferPool.Rent(source.Length * 10);
                }
                
                Span<char> charBuffer = MemoryMarshal.Cast<byte, char>(buffer.AsSpan());
                int characters = Encoding.UTF8.GetChars(source, charBuffer);
                
                analyzer._funcUtf16(analyzer, charBuffer.Slice(0, characters), ref charBuffer, ref tokens);

                characters = Encoding.UTF8.GetBytes(charBuffer, output);
                output = output.Slice(0, characters);

                // Adjust the tokens considering that we may be changing from 4 bytes to less and from 2 to 1.
                fixed (byte* outputPtrImmutable = output)
                {
                    var outputPtr = outputPtrImmutable;
                    var endPtr = outputPtr + output.Length;

                    uint excessBytes = 0;
                    uint totalExcessBytes = 0;
                    int currentToken = 0;
                    uint processedChars = 0;
                    while (outputPtr < endPtr)
                    {
                        var @byte = *outputPtr;
                        switch (@byte)
                        {
                            //http://www.unicode.org/versions/Unicode9.0.0/ch03.pdf#page=54
                            case <= 0b0111_1111:
                                /* 1 byte sequence: 0b0xxxxxxxx */
                                // Nothing to do here. 
                                break;
                            case <= 0b1101_1111:
                                /* 2 byte sequence: 0b110xxxxxx */
                                outputPtr += 1;
                                excessBytes += 1;
                                break;
                            case <= 0b1110_1111:
                                /* 0b1110xxxx: 3 bytes sequence */
                                outputPtr += 2;
                                excessBytes += 2;
                                break;
                            case <= 0b1111_0111:
                                /* 0b11110xxx: 4 bytes sequence */
                                outputPtr += 3;
                                excessBytes += 3;
                                break;
                        }
                    
                        // We have processed 1 char.
                        processedChars++;


                        ref var token = ref tokens[currentToken];
                        if (token.Length == processedChars)
                        {
                            // We need to persist original end of word (without considering UTF8 length)
                            var originalEndPtrOfWordWithoutUtf8Analysis = token.Length + token.Offset;

                            // We update the length of the current token. 
                            token.Length += excessBytes;

                            // We also update the offset considering all the excess bytes we accumulated. 
                            token.Offset += (int)totalExcessBytes;

                            // We update the total excess bytes for the next. 
                            totalExcessBytes += excessBytes;

                            //Moving to another word
                            currentToken++;

                            //Clearing
                            excessBytes = 0;
                            processedChars = 0;

                            // We've found right tokens in buffer.
                            if (currentToken >= tokens.Length)
                                break;

                            // We need to escape the space between current word and the next one. Eg. spaces in WhitespaceTokenizer.
                            var nextToken = tokens[currentToken];
                            var charsToSkip = nextToken.Offset - originalEndPtrOfWordWithoutUtf8Analysis;
                            if (originalEndPtrOfWordWithoutUtf8Analysis > 0)
                                outputPtr += charsToSkip;
                        }

                        // We need to move our base ptr
                        outputPtr++;
                    }
                }
            }

            bool canRunUtf8 = transform1.SupportUtf8 & transform2.SupportUtf8 & transform3.SupportUtf8 & tokenizer.SupportUtf8;
            if (!canRunUtf8)
                funcUtf8 = &RunUtf8WithConversion;

            return new Analyzer(funcUtf8, funcUtf16, tokenizer, new ITransformer[] { transform1, transform2, transform3 });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Analyzer Create<TTokenizer, TTransform1>(ByteStringContext context, 
                            in TTokenizer tokenizer = default(TTokenizer),
                            in TTransform1 transform1 = default(TTransform1))
            where TTokenizer : ITokenizer
            where TTransform1 : ITransformer
        {
            return Create<TTokenizer, TTransform1, NullTransformer, NullTransformer>(context, tokenizer, transform1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Analyzer Create<TTokenizer, TTransform1, TTransform2>(ByteStringContext context,
                            in TTokenizer tokenizer = default(TTokenizer),
                            in TTransform1 transform1 = default(TTransform1),
                            in TTransform2 transform2 = default(TTransform2))
            where TTokenizer : ITokenizer
            where TTransform1 : ITransformer
            where TTransform2 : ITransformer
        {
            return Create<TTokenizer, TTransform1, TTransform2, NullTransformer>(context, tokenizer, transform1, transform2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Analyzer With<TTransform1>(in TTransform1 transform1 = default(TTransform1))
            where TTransform1 : ITransformer
        {
            delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, ref byte[], void> funcUtf8 = &RunUtf8<NullTokenizer, TTransform1, NullTransformer, NullTransformer>;
            delegate*<Analyzer, ReadOnlySpan<char>, ref Span<char>, ref Span<Token>, void> funcUtf16 = &RunUtf16<NullTokenizer, TTransform1, NullTransformer, NullTransformer>;

            return new Analyzer(this, funcUtf8, funcUtf16, default(NullTokenizer), new ITransformer[] { transform1 });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Analyzer With<TTransform1, TTransform2>(
                            in TTransform1 transform1 = default(TTransform1),
                            in TTransform2 transform2 = default(TTransform2))
            where TTransform1 : ITransformer
            where TTransform2 : ITransformer
        {
            delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, ref byte[], void> funcUtf8 = &RunUtf8<NullTokenizer, TTransform1, TTransform2, NullTransformer>;
            delegate*<Analyzer, ReadOnlySpan<char>, ref Span<char>, ref Span<Token>, void> funcUtf16 = &RunUtf16<NullTokenizer, TTransform1, TTransform2, NullTransformer>;

            return new Analyzer(this, funcUtf8, funcUtf16, default(NullTokenizer), new ITransformer[] { transform1, transform2 });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Analyzer With<TTransform1, TTransform2, TTransform3>(in TTransform1 transform1 = default(TTransform1), 
                            in TTransform2 transform2 = default(TTransform2), 
                            in TTransform3 transform3 = default(TTransform3))
            where TTransform1 : ITransformer
            where TTransform2 : ITransformer
            where TTransform3 : ITransformer
        {
            delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, ref byte[], void> funcUtf8 = &RunUtf8<NullTokenizer, TTransform1, TTransform2, TTransform3>;
            delegate*<Analyzer, ReadOnlySpan<char>, ref Span<char>, ref Span<Token>, void> funcUtf16 = &RunUtf16<NullTokenizer, TTransform1, TTransform2, TTransform3>;

            return new Analyzer(this, funcUtf8, funcUtf16, default(NullTokenizer), new ITransformer[] { transform1, transform2, transform3 });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute(ReadOnlySpan<byte> source, ref Span<byte> output, ref Span<Token> tokens, ref byte[] utf8ConversionBuffer)
        {
            if (output.Length < (int)(_sourceBufferMultiplier * source.Length))
                throw new ArgumentException("Buffer is too small");
            if (tokens.Length < (int)(_tokenBufferMultiplier * source.Length))
                throw new ArgumentException("Buffer is too small");

                    
            _funcUtf8(this, source, ref output, ref tokens, ref utf8ConversionBuffer);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute(ReadOnlySpan<byte> source, ref Span<byte> output, ref Span<Token> tokens)
        {
            if (output.Length < (int)(_sourceBufferMultiplier * source.Length))
                throw new ArgumentException("Buffer is too small");
            if (tokens.Length < (int)(_tokenBufferMultiplier * source.Length))
                throw new ArgumentException("Buffer is too small");
            
            var emptyHandler = Array.Empty<byte>();
            if (source.Length == 0)
            {
                output = Span<byte>.Empty;
                tokens = Span<Token>.Empty;
                return;
            }

            _funcUtf8(this, source, ref output, ref tokens, ref emptyHandler);
            
            if (emptyHandler.Length > 0)
                BufferPool.Return(emptyHandler);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute(ReadOnlySpan<char> source, ref Span<char> output, ref Span<Token> tokens)
        {
            Debug.Assert(output.Length >= (int)(_sourceBufferMultiplier * source.Length));
            Debug.Assert(tokens.Length >= (int)(_tokenBufferMultiplier * source.Length));

            _funcUtf16(this, source, ref output, ref tokens);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {

                }

                _disposedValue = true;
            }
        }

        ~Analyzer()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
