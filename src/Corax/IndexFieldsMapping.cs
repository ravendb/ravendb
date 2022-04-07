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
    public static readonly IndexFieldsMapping Instance = new IndexFieldsMapping(null);

    private readonly ByteStringContext _context;
    private readonly Dictionary<Slice, IndexFieldBinding> _fields;
    private readonly Dictionary<int, IndexFieldBinding> _fieldsById;
    private readonly List<IndexFieldBinding> _fieldsList;
    public int Count => _fieldsById.Count;
    public Analyzer DefaultAnalyzer;

    public IndexFieldsMapping(ByteStringContext context)
    {
        _context = context;
        _fields = new Dictionary<Slice, IndexFieldBinding>(SliceComparer.Instance);
        _fieldsById = new Dictionary<int, IndexFieldBinding>();
        _fieldsList = new List<IndexFieldBinding>();
    }

    public IndexFieldsMapping AddBinding(int fieldId, Slice fieldName, Analyzer analyzer = null, bool hasSuggestion = false, FieldIndexingMode fieldIndexingMode = FieldIndexingMode.Normal)
    {
        if (!_fieldsById.TryGetValue(fieldId, out var storedAnalyzer))
        {
            var binding = new IndexFieldBinding(fieldId, fieldName, analyzer, hasSuggestion, fieldIndexingMode);
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
            if (ifb.FieldIndexingMode == FieldIndexingMode.Exact)
                continue;

            ifb.Analyzer ??= analyzers.DefaultAnalyzer;
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
    public Corax.Analyzer Analyzer;
    public readonly bool HasSuggestions;
    public readonly FieldIndexingMode FieldIndexingMode; 

    public IndexFieldBinding(int fieldId, Slice fieldName, Analyzer analyzer = null, bool hasSuggestions = false, FieldIndexingMode fieldIndexingMode = FieldIndexingMode.Normal)
    {
        FieldId = fieldId;
        FieldName = fieldName;
        Analyzer = analyzer;
        HasSuggestions = hasSuggestions;
        FieldIndexingMode = fieldIndexingMode;
    }
}
