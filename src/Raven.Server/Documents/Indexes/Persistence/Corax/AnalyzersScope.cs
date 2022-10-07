using System;
using System.Collections.Generic;
using System.IO;
using Corax;
using Corax.Pipeline;
using Voron;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal class AnalyzersScope : IDisposable
{
    private IndexDynamicFieldsMapping _dynamicFields;
    private readonly IndexFieldsMapping _knownFields;
    private Token[] _tokensBuffer;
    private byte[] _outputBuffer;
    private readonly IndexSearcher _indexSearcher;
    private Dictionary<Slice, Analyzer> _analyzersCache;


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

    public void Execute(Slice name, ReadOnlySpan<byte> source, out ReadOnlySpan<byte> buffer, out ReadOnlySpan<Token> tokens)
    {
        Analyzer analyzer = GetAnalyzer(name);

        analyzer.GetOutputBuffersSize(source.Length, out var outputSize, out var tokensSize);

        if ((_tokensBuffer?.Length ?? 0) < tokensSize && (_outputBuffer?.Length ?? 0 ) < outputSize)
            UnlikelyGrowBuffer(outputSize, tokensSize);

        var bufferOutput = _outputBuffer.AsSpan();
        var tokenOutput = _tokensBuffer.AsSpan();
        analyzer.Execute(source, ref bufferOutput, ref tokenOutput);


        buffer = bufferOutput;
        tokens = tokenOutput;
    }

    private Analyzer GetAnalyzer(Slice name)
    {
        Analyzer analyzer;
        if (_analyzersCache.ContainsKey(name))
            return _analyzersCache[name];

        if (_knownFields.TryGetByFieldName(name, out var binding))
        {
            analyzer = binding.Analyzer;
        }
        else
        {
            if (_dynamicFields is null)
                throw new InvalidDataException($"Cannot find field {name.ToString()} inside known binding and also index doesn't contain dynamic fields. The field you try to read is not inside index.");
            var mode = _indexSearcher.GetFieldIndexingModeForDynamic(name);
            analyzer = mode switch
            {
                FieldIndexingMode.Normal => _dynamicFields.DefaultAnalyzer,
                FieldIndexingMode.Search => _dynamicFields.DefaultSearchAnalyzer(name.ToString()),
                FieldIndexingMode.No => Analyzer.DefaultAnalyzer,
                FieldIndexingMode.Exact => _dynamicFields.DefaultExactAnalyzer(name.ToString())
            };
        }

        _analyzersCache[name] = analyzer;
        
        return analyzer;
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
