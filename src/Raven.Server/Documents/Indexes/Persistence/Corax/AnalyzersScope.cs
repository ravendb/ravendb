using System;
using System.Collections.Generic;
using System.IO;
using Corax;
using Corax.Pipeline;
using Voron;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal class AnalyzersScope : IDisposable
{
    private readonly IndexDynamicFieldsMapping _dynamicFields;
    private readonly IndexFieldsMapping _knownFields;
    private Token[] _tokensBuffer;
    private byte[] _outputBuffer;
    private readonly IndexSearcher _indexSearcher;
    private readonly Dictionary<Slice, Analyzer> _analyzersCache;
    
    public AnalyzersScope(IndexSearcher indexSearcher, IndexFieldsMapping fieldsMapping, bool hasDynamics)
    {
        _indexSearcher = indexSearcher;
        _knownFields = fieldsMapping;
        _analyzersCache = new(SliceComparer.Instance);

        if (hasDynamics)
        {
            _dynamicFields = fieldsMapping.CreateIndexMappingForDynamic();
        }
    }

    public void Execute(Slice fieldName, ReadOnlySpan<byte> source, out ReadOnlySpan<byte> buffer, out ReadOnlySpan<Token> tokens)
    {
        Analyzer analyzer = GetAnalyzer(fieldName);

        analyzer.GetOutputBuffersSize(source.Length, out var outputSize, out var tokensSize);

        if ((_tokensBuffer?.Length ?? 0) < tokensSize && (_outputBuffer?.Length ?? 0 ) < outputSize)
            UnlikelyGrowBuffer(outputSize, tokensSize);

        var bufferOutput = _outputBuffer.AsSpan();
        var tokenOutput = _tokensBuffer.AsSpan();
        analyzer.Execute(source, ref bufferOutput, ref tokenOutput);


        buffer = bufferOutput;
        tokens = tokenOutput;
    }

    private Analyzer GetAnalyzer(Slice fieldName)
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
            if (_dynamicFields is null)
                ThrowWhenDynamicFieldNotFound(fieldName);
            var mode = _indexSearcher.GetFieldIndexingModeForDynamic(fieldName);
            
            analyzer = mode switch
            {
                FieldIndexingMode.Normal => _dynamicFields!.DefaultAnalyzer,
                FieldIndexingMode.Search => _dynamicFields!.DefaultSearchAnalyzer(fieldName.ToString()),
                FieldIndexingMode.No => Analyzer.DefaultAnalyzer,
                FieldIndexingMode.Exact => _dynamicFields!.DefaultExactAnalyzer(fieldName.ToString()),
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
