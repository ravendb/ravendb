using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Voron;

namespace Corax;

public class IndexFieldsMapping : IEnumerable<IndexFieldBinding>
{
    private readonly ByteStringContext _context;
    private readonly Dictionary<Slice, IndexFieldBinding> _fields;
    private readonly Dictionary<int, IndexFieldBinding> _fieldsById;
    private readonly List<IndexFieldBinding> _fieldsList;
    public int Count => _fieldsById.Count;
    public Analyzer DefaultAnalyzer;
    private int _maximumOutputSize;
    public int MaximumOutputSize => _maximumOutputSize;
    
    private int _maximumTokenSize;
    public int MaximumTokenSize => _maximumTokenSize;

    public IndexFieldsMapping(ByteStringContext context)
    {
        _context = context;
        _fields = new Dictionary<Slice, IndexFieldBinding>(SliceComparer.Instance);
        _fieldsById = new Dictionary<int, IndexFieldBinding>();
        _fieldsList = new List<IndexFieldBinding>();
    }

    
    private const short LongSuffix = 19501; //"-L"
    private const short DoubleSuffix = 17453; //"-D"

    public ByteStringContext<ByteStringMemoryCache>.InternalScope GetFieldNameForLongs(Slice fieldName, out Slice fieldNameForLongs)
    {
        return GetFieldNameWithPostfix(fieldName, LongSuffix, out fieldNameForLongs);
    }
    
    public ByteStringContext<ByteStringMemoryCache>.InternalScope GetFieldNameForDoubles(Slice fieldName, out Slice fieldNameForDoubles)
    {
        return GetFieldNameWithPostfix(fieldName, DoubleSuffix, out fieldNameForDoubles);
    }

    private unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope GetFieldNameWithPostfix(Slice fieldName, short postfix, out Slice fieldWithPostfix)
    {
        var scope = _context.Allocate(fieldName.Size + sizeof(short), out ByteString output);
        fieldName.Content.CopyTo(output.Ptr);
        *(short*)(output.Ptr + fieldName.Size) = postfix;
        fieldWithPostfix = new Slice(SliceOptions.Key, output);
        return scope;
    }

    public IndexFieldsMapping AddBinding(int fieldId, Slice fieldName, Analyzer analyzer = null, bool hasSuggestion = false, FieldIndexingMode fieldIndexingMode = FieldIndexingMode.Normal, bool hasSpatial = false)
    {
        if (!_fieldsById.TryGetValue(fieldId, out var storedAnalyzer))
        {
            GetFieldNameForDoubles(fieldName, out var fieldNameDouble);
            GetFieldNameForLongs(fieldName, out var fieldNameLong);
            var binding = new IndexFieldBinding(fieldId, fieldName,  fieldNameLong,fieldNameDouble, 
                analyzer, hasSuggestion, fieldIndexingMode, hasSpatial);
            _fields[fieldName] = binding;
            _fieldsById[fieldId] = binding;
            _fieldsList.Add(binding);
        }
        else
        {
            Debug.Assert(analyzer == storedAnalyzer.Analyzer);
        }

        return this;
    }

    public void UpdateAnalyzersInBindings(IndexFieldsMapping analyzers)
    {
        foreach (var mapping in analyzers.GetEnumerator())
        {
            if (TryGetByFieldId(mapping.FieldId, out var binding) == true)
            {
                binding.Analyzer = mapping.Analyzer;
            }
        }

        foreach (var ifb in CollectionsMarshal.AsSpan(_fieldsList))
        {
            if (ifb.FieldIndexingMode == FieldIndexingMode.Exact || ifb.HasSpatial == true)
                continue;

            ifb.Analyzer ??= analyzers.DefaultAnalyzer;
        }
        
        //We want also find maximum buffer for analyzers.
        UpdateMaximumOutputAndTokenSize();
    }

    internal void UpdateMaximumOutputAndTokenSize()
    {
        foreach (var analyzer in CollectionsMarshal.AsSpan(_fieldsList))
        {
            if (analyzer.Analyzer == null)
                continue;
            
            _maximumOutputSize = Math.Max(_maximumOutputSize, analyzer.Analyzer.DefaultOutputSize);
            _maximumTokenSize = Math.Max(_maximumTokenSize, analyzer.Analyzer.DefaultTokenSize);
        }
    }
        
    
    public IndexFieldBinding GetByFieldId(int fieldId)
    {
        return _fieldsById[fieldId];
    }

    public bool TryGetByFieldId(int fieldId, out IndexFieldBinding binding)
    {
        return _fieldsById.TryGetValue(fieldId, out binding);
    }

    public IndexFieldBinding GetByFieldName(string fieldName)
    {
        // This method is a convenience method that should not be used in high performance sections of the code.
        using var _ = Slice.From(_context, fieldName, out var str);
        return _fields[str];
    }

    public bool TryGetByFieldName(string fieldName, out IndexFieldBinding binding)
    {
        // This method is a convenience method that should not be used in high performance sections of the code.
        using var _ = Slice.From(_context, fieldName, out var str);
        return _fields.TryGetValue(str, out binding);
    }

    public IndexFieldBinding GetByFieldName(Slice fieldName)
    {
        return _fields[fieldName];
    }

    public bool TryGetByFieldName(Slice fieldName, out IndexFieldBinding binding)
    {
        return _fields.TryGetValue(fieldName, out binding);
    }

    public IEnumerator<IndexFieldBinding> GetEnumerator()
    {
        return _fieldsList.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return _fieldsList.GetEnumerator();
    }
}

public class IndexFieldBinding
{
    public readonly int FieldId;
    public readonly Slice FieldName;
    public readonly Slice FieldNameLong;
    public readonly Slice FieldNameDouble;
    public Corax.Analyzer Analyzer;
    public readonly bool HasSuggestions;
    public readonly bool HasSpatial;
    public FieldIndexingMode FieldIndexingMode => _silentlyChangedIndexingMode ?? _fieldIndexingMode;
    private readonly FieldIndexingMode _fieldIndexingMode;
    private FieldIndexingMode? _silentlyChangedIndexingMode;
    private string _fieldName;

    public IndexFieldBinding(int fieldId, Slice fieldName, Slice fieldNameLong,Slice fieldNameDouble,
        Analyzer analyzer = null, bool hasSuggestions = false, 
        FieldIndexingMode fieldIndexingMode = FieldIndexingMode.Normal, 
        bool hasSpatial = false)
    {
        FieldId = fieldId;
        FieldName = fieldName;
        FieldNameDouble = fieldNameDouble;
        FieldNameLong = fieldNameLong;
        Analyzer = analyzer;
        HasSuggestions = hasSuggestions;
        _fieldIndexingMode = fieldIndexingMode;
        HasSpatial = hasSpatial;
        IsAnalyzed = Analyzer is not null && FieldIndexingMode is not FieldIndexingMode.Exact && HasSpatial is false;
    }

    public string FieldNameAsString
    {
        get
        {
            return _fieldName ??= FieldName.ToString();
        }
    }

    public bool IsAnalyzed;

    public bool IsIndexed
    {
        get
        {
            return FieldIndexingMode != FieldIndexingMode.No;
        }
    }

    public void OverrideFieldIndexingMode(FieldIndexingMode mode)
    {
        _silentlyChangedIndexingMode = mode;
    }
}
