using Corax.Analyzers;
using Voron;

namespace Corax.Fields;

public class IndexFieldBinding
{
    public readonly int FieldId;
    public readonly Slice FieldName;
    public readonly Slice FieldNameLong;
    public readonly Slice FieldNameDouble;
    public Analyzer Analyzer;
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
    }

    public string FieldNameAsString
    {
        get
        {
            return _fieldName ??= FieldName.ToString();
        }
    }
    
    public bool IsAnalyzed
    {
        get
        {
            return Analyzer is not null && FieldIndexingMode is not FieldIndexingMode.Exact && HasSpatial is false;
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
        _silentlyChangedIndexingMode = mode;
    }
}
