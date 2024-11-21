using System;
using Corax.Analyzers;
using Voron;
using Corax.Indexing;

namespace Corax.Mappings;

public sealed class IndexFieldBinding
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
    public FieldIndexingMode FieldIndexingMode => _silentlyChangedIndexingModeLegacy ?? field;
    public readonly bool ShouldStore;
    private FieldIndexingMode? _silentlyChangedIndexingModeLegacy;

    private readonly bool _isFieldBindingForWriter;
    public readonly FieldMetadata Metadata;
    public VectorOptions VectorOptions;

    public IndexFieldBinding(int fieldId, Slice fieldName, Slice fieldNameLong, Slice fieldNameDouble, Slice fieldTermTotalSumField, bool isFieldBindingForWriter,
        Analyzer analyzer = null, bool hasSuggestions = false,
        FieldIndexingMode fieldIndexingMode = FieldIndexingMode.Normal,
        bool shouldStore = false,
        bool hasSpatial = false, VectorOptions vectorOptions = null)
    {
        FieldId = fieldId;
        FieldName = fieldName;
        FieldNameDouble = fieldNameDouble;
        FieldNameLong = fieldNameLong;
        FieldTermTotalSumField = fieldTermTotalSumField;
        HasSuggestions = hasSuggestions;
        FieldIndexingMode = fieldIndexingMode;
        ShouldStore = shouldStore;
        HasSpatial = hasSpatial;
        _isFieldBindingForWriter = isFieldBindingForWriter;
        _analyzer = analyzer;
        VectorOptions = vectorOptions;
        Metadata = FieldMetadata.Build(fieldName, fieldTermTotalSumField, fieldId, fieldIndexingMode, analyzer);
    }
    
    public string FieldNameAsString
    {
        get
        {
            return field ??= FieldName.ToString();
        }
    }

    public string FieldNameForStatistics
    {
        get
        {
            return field ??= $"Field_{FieldName}";
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
        
        _silentlyChangedIndexingModeLegacy = mode;
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
