using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Mappings;
using Corax.Pipeline;
using Voron;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal class AnalyzersScope : IDisposable
{
    private readonly IndexFieldsMapping _knownFields;
    private readonly bool _hasDynamics;
    private Token[] _tokensBuffer;
    private byte[] _outputBuffer;
    private readonly IndexSearcher _indexSearcher;
    private readonly Dictionary<Slice, Analyzer> _analyzersCache;
    
    public AnalyzersScope(IndexSearcher indexSearcher, IndexFieldsMapping fieldsMapping, bool hasDynamics)
    {
        _indexSearcher = indexSearcher;
        _knownFields = fieldsMapping;
        _hasDynamics = hasDynamics;
        _analyzersCache = new(SliceComparer.Instance);
        UnlikelyGrowBuffer(128, 128);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Execute(Slice fieldName, ReadOnlySpan<byte> source, out ReadOnlySpan<byte> buffer, out ReadOnlySpan<Token> tokens)
    {
        Analyzer analyzer = GetAnalyzer(fieldName);
        ExecuteAnalyze(analyzer, source, out buffer, out tokens);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Execute(FieldMetadata field, ReadOnlySpan<byte> source, out ReadOnlySpan<byte> buffer, out ReadOnlySpan<Token> tokens, bool exact)
    {
        
        if (field.Mode == FieldIndexingMode.Exact || field.Analyzer == null || exact)
        {
            buffer = source;
            _tokensBuffer[0] = new Token() {Length = (uint)source.Length, Offset = 0, Type = TokenType.Term};
            tokens = _tokensBuffer.AsSpan(0, 1);
            return;
        }

        var analyzer = field.Analyzer;
        
        ExecuteAnalyze(analyzer, source, out buffer, out tokens);
    }
    
    private void ExecuteAnalyze(Analyzer analyzer, ReadOnlySpan<byte> source, out ReadOnlySpan<byte> buffer, out ReadOnlySpan<Token> tokens)
    {
        analyzer.GetOutputBuffersSize(source.Length, out var outputSize, out var tokensSize);

        if ((_tokensBuffer?.Length ?? 0) < tokensSize && (_outputBuffer?.Length ?? 0 ) < outputSize)
            UnlikelyGrowBuffer(outputSize, tokensSize);

        var bufferOutput = _outputBuffer.AsSpan();
        var tokenOutput = _tokensBuffer.AsSpan();
        analyzer.Execute(source, ref bufferOutput, ref tokenOutput);


        buffer = bufferOutput;
        tokens = tokenOutput;
    }

    private Analyzer GetAnalyzer(Slice fieldName, FieldMetadata field = default)
    {
        Analyzer analyzer;
        if (_analyzersCache.ContainsKey(fieldName))
            return _analyzersCache[fieldName];

        if (_knownFields.TryGetByFieldName(fieldName, out var binding))
        {
            analyzer = binding.Analyzer;
        }
        else
        {
            if (_hasDynamics is false)
                ThrowWhenDynamicFieldNotFound(fieldName);
            var mode = _indexSearcher.GetFieldIndexingModeForDynamic(fieldName);
            
            analyzer = mode switch
            {
                FieldIndexingMode.Normal => _knownFields!.DefaultAnalyzer,
                FieldIndexingMode.Search => _knownFields!.SearchAnalyzer(fieldName.ToString()),
                FieldIndexingMode.No => Analyzer.CreateDefaultAnalyzer( _indexSearcher.Allocator),
                FieldIndexingMode.Exact => _knownFields!.ExactAnalyzer(fieldName.ToString()),
                _ => ThrowWhenAnalyzerModeNotFound(mode)
            };
        }

        _analyzersCache[fieldName] = analyzer;
        
        return analyzer;
    }

    private static Analyzer ThrowWhenAnalyzerModeNotFound(FieldIndexingMode mode)
    {
        throw new ArgumentOutOfRangeException($"{mode} is not implemented in {nameof(AnalyzersScope)}");
    }

    private static void ThrowWhenDynamicFieldNotFound(Slice fieldName)
    {
        throw new InvalidDataException(
            $"Cannot find field {fieldName.ToString()} inside known binding and also index doesn't contain dynamic fields. The field you try to read is not inside index.");
    }

    private void UnlikelyGrowBuffer(int outputSize, int tokensSize)
    {
        ReturnBuffers();

        _outputBuffer = Analyzer.BufferPool.Rent(outputSize);
        _tokensBuffer = Analyzer.TokensPool.Rent(tokensSize);
    }

    private void ReturnBuffers()
    {
        if (_tokensBuffer != null)
        {
            Analyzer.TokensPool.Return(_tokensBuffer);
            _tokensBuffer = null;
        }

        if (_outputBuffer != null)
        {
            Analyzer.BufferPool.Return(_outputBuffer);
            _outputBuffer = null;
        }
    }

    public void Dispose()
    {
        ReturnBuffers();
    }
}
