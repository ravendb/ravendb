using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Corax.Analyzers;
using Sparrow.Server;
using Voron;

namespace Corax.Mappings;

public sealed class IndexFieldsMapping : IEnumerable<IndexFieldBinding>, IDisposable
{
    private readonly Dictionary<Slice, IndexFieldBinding> _fields;
    private readonly Dictionary<string, IndexFieldBinding> _fieldsByString;
    private readonly Dictionary<int, IndexFieldBinding> _fieldsById;
    private readonly ByteStringContext _mappingContext;
    public readonly Func<string, Analyzer> ExactAnalyzer;
    public readonly Func<string, Analyzer> SearchAnalyzer;
    public readonly Analyzer DefaultAnalyzer;
    public readonly int MaximumOutputSize;
    public readonly int MaximumTokenSize;
    public int Count => _fields.Count;
    
    internal IndexFieldsMapping(ByteStringContext mappingContext, Dictionary<Slice, IndexFieldBinding> fields, Dictionary<int, IndexFieldBinding> fieldsById, Analyzer defaultAnalyzer, Func<string, Analyzer> searchAnalyzer, Func<string, Analyzer> exactAnalyzer, int maximumOutputSize, int maximumTokenSize)
    {
        _mappingContext = mappingContext;
        _fields = fields;
        _fieldsById = fieldsById;
        DefaultAnalyzer = defaultAnalyzer;
        SearchAnalyzer = searchAnalyzer;
        ExactAnalyzer = exactAnalyzer;
        MaximumOutputSize = maximumOutputSize;
        MaximumTokenSize = maximumTokenSize;

        _fieldsByString = fields.ToDictionary(x => x.Key.ToString(), x => x.Value);
    }
   
    public bool TryGetByFieldName(ByteStringContext context, string fieldName, out IndexFieldBinding binding)
    {
        using var _ = Slice.From(context, fieldName, out var slicedName);
        return TryGetByFieldName(slicedName, out binding);
    }

    public bool TryGetByFieldName(ByteStringContext context, Span<byte> fieldName, out IndexFieldBinding binding)
    {
        using var _ = Slice.From(context, fieldName, out var slicedName);
        return TryGetByFieldName(slicedName, out binding);
    }
    
    public bool TryGetByFieldName(string fieldName, out IndexFieldBinding binding) => _fieldsByString.TryGetValue(fieldName, out binding);

    public bool ContainsField(string fieldName) => _fieldsByString.ContainsKey(fieldName);
    
    public bool TryGetByFieldName(Slice fieldName, out IndexFieldBinding binding) => _fields.TryGetValue(fieldName, out binding);

    public bool TryGetByFieldId(int fieldId, out IndexFieldBinding binding) => _fieldsById.TryGetValue(fieldId, out binding);

    public IndexFieldBinding GetFirstField() => _fieldsById.First().Value;
    
    public IndexFieldBinding GetByFieldId(int fieldId) => _fieldsById[fieldId];
    
    public IEnumerator<IndexFieldBinding> GetEnumerator()
    {
        return _fields.Values.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
    
    public void Dispose()
    {
        _mappingContext?.Dispose();
    }

    // This is stored as last "known" field always..
    public int StoredJsonPropertyOffset => _fields.Count - 1;
}
