using System;
using Voron;

namespace Corax.Mappings;

public class IndexFieldBinding
{
    public readonly int FieldId;
    public readonly Slice FieldName;
    public readonly Slice FieldNameLong;
    public readonly Slice FieldNameDouble;
    public readonly Slice FieldTermTotalSumField;
    public Analyzer Analyzer => _analyzer;
    private Analyzer _analyzer;
    public readonly bool HasSuggestions;
    public readonly bool HasSpatial;
    public FieldIndexingMode FieldIndexingMode => _silentlyChangedIndexingMode ?? _fieldIndexingMode;
    private readonly FieldIndexingMode _fieldIndexingMode;
    private FieldIndexingMode? _silentlyChangedIndexingMode;
    private string _fieldName;
    
    private readonly bool _isFieldBindingForWriter;

    public readonly FieldMetadata Metadata;
    
    public IndexFieldBinding(int fieldId, Slice fieldName, Slice fieldNameLong, Slice fieldNameDouble, Slice fieldTermTotalSumField, bool isFieldBindingForWriter,
        Analyzer analyzer = null, bool hasSuggestions = false,
        FieldIndexingMode fieldIndexingMode = FieldIndexingMode.Normal,
        bool hasSpatial = false)
    {
        FieldId = fieldId;
        FieldName = fieldName;
        FieldNameDouble = fieldNameDouble;
        FieldNameLong = fieldNameLong;
        FieldTermTotalSumField = fieldTermTotalSumField;
        HasSuggestions = hasSuggestions;
        _fieldIndexingMode = fieldIndexingMode;
        HasSpatial = hasSpatial;
        _isFieldBindingForWriter = isFieldBindingForWriter;
        _analyzer = analyzer;
        Metadata = FieldMetadata.Build(fieldName, fieldTermTotalSumField, fieldId, fieldIndexingMode, analyzer);
    }

    public string FieldNameAsString
    {
        get
        {
            return _fieldName ??= FieldName.ToString();
        }
    }
    
    public bool IsIndexed
    {
        get
        {
            return FieldIndexingMode != FieldIndexingMode.No;
        }
    }

    public void OverrideFieldIndexingMode(FieldIndexingMode mode)
    {
        AssertBindingIsMadeForIndexing();
        
        _silentlyChangedIndexingMode = mode;
    }

    public void SetAnalyzer(Analyzer analyzer)
    {
        AssertBindingIsMadeForIndexing();
        
        _analyzer = analyzer;
    }
    
    private void AssertBindingIsMadeForIndexing()
    {
        if (_isFieldBindingForWriter == false)
            throw new NotSupportedException($"Only bindings made for {nameof(IndexWriter)} are mutable.");
    }
}
