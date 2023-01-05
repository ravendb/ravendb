using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Corax.Mappings;
using Corax.Pipeline;
using Sparrow.Server;
using Voron;

namespace Corax.Analyzers
{
    internal unsafe class AnalyzerScope : IDisposable
    {
        private readonly IndexSearcher _indexSearcher;
        private readonly Analyzer _analyzer;

        private ByteStringContext<ByteStringMemoryCache>.InternalScope _tempBufferScope;
        private int _tempOutputBufferSize;
        private byte* _tempOutputBuffer;
        private int _tempOutputTokenSize;
        private Token* _tempTokenBuffer;

        public AnalyzerScope(IndexSearcher indexSearcher, Analyzer analyzer)
        {
            _indexSearcher = indexSearcher;
            _analyzer = analyzer;

            InitializeTemporaryBuffers(indexSearcher.Allocator);
        }

        public AnalyzerScope(string fieldName, IndexSearcher indexSearcher, IndexFieldsMapping fieldsMapping, bool hasDynamics)
        {
            _indexSearcher = indexSearcher;

            if (fieldsMapping.TryGetByFieldName(indexSearcher.Allocator, fieldName, out var binding))
            {
                _analyzer = binding.Analyzer;
            }
            else
            {
                if (hasDynamics is false)
                    ThrowWhenDynamicFieldNotFound(fieldName);

                using var _ = Slice.From(indexSearcher.Allocator, fieldName, out var fieldNameSlice);
                var mode = _indexSearcher.GetFieldIndexingModeForDynamic(fieldNameSlice);

                _analyzer = mode switch
                {
                    FieldIndexingMode.Normal => fieldsMapping!.DefaultAnalyzer,
                    FieldIndexingMode.Search => fieldsMapping!.SearchAnalyzer(fieldName.ToString()),
                    FieldIndexingMode.No => Analyzer.CreateDefaultAnalyzer(_indexSearcher.Allocator),
                    FieldIndexingMode.Exact => fieldsMapping!.ExactAnalyzer(fieldName.ToString()),
                    _ => ThrowWhenAnalyzerModeNotFound(mode)
                };
            }

            InitializeTemporaryBuffers(indexSearcher.Allocator);
        }

        private void InitializeTemporaryBuffers(ByteStringContext allocator)
        {
            _tempOutputBufferSize = Constants.Analyzers.DefaultBufferForAnalyzers;
            _tempOutputTokenSize = Constants.Analyzers.DefaultBufferForAnalyzers;

            _tempBufferScope = allocator.AllocateDirect(_tempOutputBufferSize + _tempOutputTokenSize * Unsafe.SizeOf<Token>(), out var tempBuffer);
            _tempOutputBuffer = tempBuffer.Ptr;
            _tempTokenBuffer = (Token*)(tempBuffer.Ptr + _tempOutputBufferSize);
        }

        private void UnlikelyGrowBuffers(int outputSize, int tokenSize)
        {
            _tempBufferScope.Dispose();

            _tempOutputBufferSize = outputSize;
            _tempOutputTokenSize = tokenSize;

            _tempBufferScope = _indexSearcher.Allocator.AllocateDirect(_tempOutputBufferSize + _tempOutputTokenSize * Unsafe.SizeOf<Token>(), out var tempBuffer);
            _tempOutputBuffer = tempBuffer.Ptr;
            _tempTokenBuffer = (Token*)(tempBuffer.Ptr + _tempOutputBufferSize);
        }

        public ByteStringContext<ByteStringMemoryCache>.InternalScope Execute(ReadOnlySpan<byte> source, out Span<byte> buffer, out Span<Token> tokens)
        {
            _analyzer.GetOutputBuffersSize(source.Length, out var outputSize, out var tokensSize);

            if (outputSize > _tempOutputBufferSize || tokensSize > _tempOutputTokenSize)
                UnlikelyGrowBuffers(outputSize, tokensSize);

            var tokenSpace = new Span<Token>(_tempTokenBuffer, _tempOutputTokenSize);
            var wordSpace = new Span<byte>(_tempOutputBuffer, _tempOutputBufferSize);
            _analyzer.Execute(source, ref wordSpace, ref tokenSpace);

            var scope = _indexSearcher.Allocator.AllocateDirect(tokenSpace.Length * sizeof(Token) + wordSpace.Length, out var outputMemory);

            var outputBuffer = new Span<byte>(outputMemory.Ptr, wordSpace.Length);
            var outputTokens = new Span<Token>(outputMemory.Ptr + outputBuffer.Length, tokenSpace.Length);

            wordSpace.CopyTo(outputBuffer);
            tokenSpace.CopyTo(outputTokens);

            buffer = outputBuffer;
            tokens = outputTokens;

            return scope;
        }

        private static Analyzer ThrowWhenAnalyzerModeNotFound(FieldIndexingMode mode)
        {
            throw new ArgumentOutOfRangeException($"{mode} is not implemented in {nameof(AnalyzerScope)}");
        }

        private static void ThrowWhenDynamicFieldNotFound(string fieldName)
        {
            throw new InvalidDataException(
                $"Cannot find field {fieldName} inside known binding and also index doesn't contain dynamic fields. The field you try to read is not inside index.");
        }

        public void Dispose()
        {
            _tempBufferScope.Dispose();
        }
    }
}
