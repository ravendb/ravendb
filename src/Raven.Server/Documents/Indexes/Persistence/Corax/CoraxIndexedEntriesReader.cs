using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Corax;
using Corax.Pipeline;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class CoraxIndexedEntriesReader : IDisposable
{
    private readonly IndexFieldsMapping _fieldsMapping;

    private readonly Dictionary<string, byte[]> _dynamicMapping;
    private Token[] _tokensBuffer;
    private byte[] _outputBuffer;

    private readonly HashSet<object> _container;

    public CoraxIndexedEntriesReader(IndexSearcher indexSearcher, IndexFieldsMapping fieldsMapping)
    {
        _fieldsMapping = fieldsMapping;
        _dynamicMapping = new();

        foreach (var dynamicField in indexSearcher.GetFields())
            _dynamicMapping.Add(dynamicField, Encoding.UTF8.GetBytes(dynamicField));

        _container = new();
    }

    public DynamicJsonValue GetDocument(ref IndexEntryReader entryReader)
    {
        var doc = new DynamicJsonValue();
        foreach (var binding in _fieldsMapping)
        {
            var fieldReader = entryReader.GetReaderFor(binding.FieldId);

            if (fieldReader.Type == IndexEntryFieldType.Invalid)
                continue;

            doc[binding.FieldNameAsString] = GetValueForField(ref fieldReader, binding.Analyzer);
        }

        foreach (var (fieldAsString, fieldAsBytes) in _dynamicMapping)
        {
            var fieldReader = entryReader.GetReaderFor(fieldAsBytes);

            if (fieldReader.Type == IndexEntryFieldType.Invalid)
                continue;

            doc[fieldAsString] = GetValueForField(ref fieldReader, null);
        }

        return doc;
    }

    public object GetValueForField(ref IndexEntryReader.FieldReader fieldReader, Analyzer analyzer)
    {
        _container.Clear();
        switch (fieldReader.Type)
        {
            case IndexEntryFieldType.Empty:
                _container.Add(string.Empty);
                break;
            case IndexEntryFieldType.Null:
                _container.Add(null);
                break;
            case IndexEntryFieldType.TupleListWithNulls:
            case IndexEntryFieldType.TupleList:
                if (fieldReader.TryReadMany(out var iterator) == false)
                    break;

                while (iterator.ReadNext())
                {
                    if (iterator.IsNull)
                    {
                        _container.Add(null);
                    }
                    else if (iterator.IsEmpty)
                    {
                        throw new InvalidDataException("Tuple list cannot contain an empty string (otherwise, where did the numeric came from!)");
                    }
                    else
                    {
                        
                        AddBytesAsItemToContainer(iterator.Sequence, null);
                    }
                }

                break;
            case IndexEntryFieldType.Tuple:
                if (fieldReader.Read(out _, out long lVal, out double dVal, out Span<byte> valueInEntry) == false)
                    break;
                AddBytesAsItemToContainer(valueInEntry, null);
                
                break;
            case IndexEntryFieldType.SpatialPointList:
                if (fieldReader.TryReadManySpatialPoint(out var spatialIterator) == false)
                    break;

                while (spatialIterator.ReadNext())
                {
                    for (int i = 1; i <= spatialIterator.Geohash.Length; ++i)
                        AddBytesAsItemToContainer(spatialIterator.Geohash.Slice(0, i), null);
                }

                break;

            case IndexEntryFieldType.SpatialPoint:
                if (fieldReader.Read(out valueInEntry) == false)
                    break;

                for (int i = 1; i <= valueInEntry.Length; ++i)
                    AddBytesAsItemToContainer(valueInEntry.Slice(0, i), null);

                break;

            case IndexEntryFieldType.ListWithNulls:
            case IndexEntryFieldType.List:
                if (fieldReader.TryReadMany(out iterator) == false)
                    break;

                while (iterator.ReadNext())
                {
                    Debug.Assert((fieldReader.Type & IndexEntryFieldType.Tuple) == 0, "(fieldType & IndexEntryFieldType.Tuple) == 0");

                    if ((fieldReader.Type & IndexEntryFieldType.HasNulls) != 0 && (iterator.IsEmpty || iterator.IsNull))
                    {
                        if (iterator.IsEmpty)
                            _container.Add(string.Empty);
                        else
                            _container.Add(null);
                    }
                    else
                    {
                        AddBytesAsItemToContainer(iterator.Sequence, analyzer);
                    }
                }

                break;
            case IndexEntryFieldType.Raw:
            case IndexEntryFieldType.RawList:
            case IndexEntryFieldType.Invalid:
                break;
            default:
                fieldReader.Read(out var value);
                AddBytesAsItemToContainer(value, analyzer);
                break;
        }

        return _container.Count switch
        {
            1 => _container.First(),
            > 1 => _container.ToArray(),
            _ => string.Empty
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddBytesAsItemToContainer(ReadOnlySpan<byte> value, Analyzer analyzer)
    {
        if (analyzer is null)
        {
            _container.Add(Encodings.Utf8.GetString(value));
            return;
        }
        
        analyzer.GetOutputBuffersSize(value.Length, out var outputSize, out var tokenSize);
        UnlikelyGrowBuffer(tokenSize, outputSize);

        var output = _outputBuffer.AsSpan();
        var tokens = _tokensBuffer.AsSpan();
        analyzer.Execute(value, ref output, ref tokens);

        foreach (var token in tokens)
        {
            var analyzedTerm = output.Slice(token.Offset, (int)token.Length);
            _container.Add(Encoding.UTF8.GetString(analyzedTerm));
        }
    }

    private void UnlikelyGrowBuffer(int tokensSize, int outputSize)
    {
        if (_tokensBuffer?.Length > tokensSize && _outputBuffer?.Length > outputSize)
            return;

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

        _outputBuffer = Analyzer.BufferPool.Rent(outputSize);
        _tokensBuffer = Analyzer.TokensPool.Rent(tokensSize);
    }

    public void Dispose()
    {
        if (_tokensBuffer != null)
            Analyzer.TokensPool.Return(_tokensBuffer);
        if (_outputBuffer != null)
            Analyzer.BufferPool.Return(_outputBuffer);
    }
}
