using System;
using System.Collections;
using System.Collections.Generic;
using Sparrow.Server;
using Voron;

namespace Corax.Mappings;

public class IndexFieldsMapping : IEnumerable<IndexFieldBinding>, IDisposable
{
    private readonly Dictionary<Slice, IndexFieldBinding> _fields;
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
    }
   
    public bool TryGetByFieldName(ByteStringContext context, string fieldName, out IndexFieldBinding binding)
    {
        using var _ = Slice.From(context, fieldName, out var slicedName);
        return TryGetByFieldName(slicedName, out binding);
    }
    
    public bool TryGetByFieldName(Slice fieldName, out IndexFieldBinding binding) => _fields.TryGetValue(fieldName, out binding);

    public bool TryGetByFieldId(int fieldId, out IndexFieldBinding binding) => _fieldsById.TryGetValue(fieldId, out binding);
    
    
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
}
