using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Pipeline;

namespace Corax
{
    public unsafe class Analyzer : IDisposable
    {
        public static readonly ArrayPool<byte> BufferPool = ArrayPool<byte>.Create();
        public static readonly ArrayPool<Token> TokensPool = ArrayPool<Token>.Create();

        private readonly delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, void> _func;
        private readonly Analyzer _inner;
        private readonly ITokenizer _tokenizer;
        private readonly ITransformer[] _transformers;
        private readonly float _sourceBufferMultiplier;
        private readonly float _tokenBufferMultiplier;
        private bool _disposedValue;

        protected Analyzer(delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, void> function,
            in ITokenizer tokenizer, ITransformer[] transformers) : this(null, function, tokenizer, transformers)
        {}

        protected Analyzer(Analyzer inner,
            delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, void> function,
            in ITokenizer tokenizer, ITransformer[] transformers)
        {
            _inner = inner;
            _func = function;
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
        }

        public void GetOutputBuffersSize( int inputSize, out int outputSize, out int tokenSize )
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

            output = output.Slice(0, outputBufferSpace.Length);
            outputTokens = outputTokens.Slice(0, outputTokenSpace.Length);

            return result;
        }

        public struct NullTransformer : ITransformer
        {
            public int Transform(ReadOnlySpan<byte> source, ReadOnlySpan<Token> tokens, ref Span<byte> dest, ref Span<Token> destTokens)
            {
                throw new InvalidOperationException();
            }
        }

        public struct NullTokenizer : ITokenizer
        {
            public void Dispose()
            {
            }

            public int Tokenize(ReadOnlySpan<byte> source, ref Span<Token> tokens)
            {
                throw new InvalidOperationException();
            }
        }

        internal static void Run<TTokenizer, TTransform1, TTransform2, TTransform3>(Analyzer analyzer, 
                        ReadOnlySpan<byte> source, ref Span<byte> output, ref Span<Token> tokens)
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
        public static Analyzer Create<TTokenizer, TTransform1, TTransform2, TTransform3>(in TTokenizer tokenizer = default(TTokenizer), 
                            in TTransform1 transform1 = default(TTransform1),
                            in TTransform2 transform2 = default(TTransform2), 
                            in TTransform3 transform3 = default(TTransform3))
            where TTokenizer : ITokenizer
            where TTransform1 : ITransformer
            where TTransform2 : ITransformer
            where TTransform3 : ITransformer
        {
            return new Analyzer(&Run<TTokenizer, TTransform1, TTransform2, TTransform3>,
                tokenizer, new ITransformer[] { transform1, transform2, transform3 });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Analyzer Create<TTokenizer, TTransform1>(in TTokenizer tokenizer = default(TTokenizer),
                            in TTransform1 transform1 = default(TTransform1))
            where TTokenizer : ITokenizer
            where TTransform1 : ITransformer
        {
            return Create<TTokenizer, TTransform1, NullTransformer, NullTransformer>(tokenizer, transform1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Analyzer Create<TTokenizer, TTransform1, TTransform2>(in TTokenizer tokenizer = default(TTokenizer),
                            in TTransform1 transform1 = default(TTransform1),
                            in TTransform2 transform2 = default(TTransform2))
            where TTokenizer : ITokenizer
            where TTransform1 : ITransformer
            where TTransform2 : ITransformer
        {
            return Create<TTokenizer, TTransform1, TTransform2, NullTransformer>(tokenizer, transform1, transform2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Analyzer With<TTransform1>(in TTransform1 transform1 = default(TTransform1))
            where TTransform1 : ITransformer
        {
            return new Analyzer(this, &Run<NullTokenizer, TTransform1, NullTransformer, NullTransformer>,
                default(NullTokenizer), new ITransformer[] { transform1 });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Analyzer With<TTransform1, TTransform2>(
                            in TTransform1 transform1 = default(TTransform1),
                            in TTransform2 transform2 = default(TTransform2))
            where TTransform1 : ITransformer
            where TTransform2 : ITransformer
        {
            return new Analyzer(this, &Run<NullTokenizer, TTransform1, TTransform2, NullTransformer>,
                default(NullTokenizer), new ITransformer[] { transform1, transform2 });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Analyzer With<TTransform1, TTransform2, TTransform3>(in TTransform1 transform1 = default(TTransform1), 
                            in TTransform2 transform2 = default(TTransform2), 
                            in TTransform3 transform3 = default(TTransform3))
            where TTransform1 : ITransformer
            where TTransform2 : ITransformer
            where TTransform3 : ITransformer
        {
            return new Analyzer(this, &Run<NullTokenizer, TTransform1, TTransform2, TTransform3>,
                default(NullTokenizer), new ITransformer[] { transform1, transform2, transform3 });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute(ReadOnlySpan<byte> source, ref Span<byte> output, ref Span<Token> tokens)
        {
            Debug.Assert(tokens.Length >= (int)(_sourceBufferMultiplier * source.Length));
            Debug.Assert(output.Length >= (int)(_tokenBufferMultiplier * source.Length));

            _func(this, source, ref output, ref tokens);
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
