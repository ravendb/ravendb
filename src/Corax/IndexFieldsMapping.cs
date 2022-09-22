using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Voron;

namespace Corax;

public abstract class IndexFieldsMappingBase : IEnumerable<IndexFieldBinding>
{
    protected const short LongSuffix = 19501; //"-L"
    protected const short DoubleSuffix = 17453; //"-D"
    
    protected readonly ByteStringContext Context;
    
    protected readonly Dictionary<Slice, IndexFieldBinding> Fields;
    protected readonly Dictionary<int, IndexFieldBinding> FieldsById;
    protected readonly List<IndexFieldBinding> FieldsList;
    
    public int Count => FieldsById.Count;
    
    protected int _maximumOutputSize;
    public int MaximumOutputSize => _maximumOutputSize;
    
    protected int _maximumTokenSize;
    public int MaximumTokenSize => _maximumTokenSize;

    public readonly Func<string, Analyzer> DefaultSearchAnalyzer;
    public readonly Func<string, Analyzer> DefaultExactAnalyzer;
    public Analyzer DefaultAnalyzer;

    public IndexFieldsMappingBase(ByteStringContext context, Func<string, Analyzer> exactAnalyzer, Func<string, Analyzer> searchAnalyzer)
    {
        Context = context;
        Context = context;
        Fields = new Dictionary<Slice, IndexFieldBinding>(SliceComparer.Instance);
        FieldsById = new Dictionary<int, IndexFieldBinding>();
        FieldsList = new List<IndexFieldBinding>();
        DefaultSearchAnalyzer = searchAnalyzer;
        DefaultExactAnalyzer = exactAnalyzer;
    }
    
    public static void GetFieldNameForLongs(ByteStringContext context, Slice fieldName, out Slice fieldNameForLongs)
    {
        GetFieldNameWithPostfix(context, fieldName, LongSuffix, out fieldNameForLongs);
    }
    
    public static void GetFieldNameForDoubles(ByteStringContext context, Slice fieldName, out Slice fieldNameForDoubles)
    {
        GetFieldNameWithPostfix(context, fieldName, DoubleSuffix, out fieldNameForDoubles);
    }

    private static unsafe void GetFieldNameWithPostfix(ByteStringContext context, Slice fieldName, short postfix, out Slice fieldWithPostfix)
    {
        context.Allocate(fieldName.Size + sizeof(short), out ByteString output);
        fieldName.Content.CopyTo(output.Ptr);
        *(short*)(output.Ptr + fieldName.Size) = postfix;
        fieldWithPostfix = new Slice(SliceOptions.Key, output);
    }
    
    public void CompleteAnalyzerGathering()
    {
        foreach (var ifb in CollectionsMarshal.AsSpan(FieldsList))
        {
            if (ifb.FieldIndexingMode == FieldIndexingMode.Exact || ifb.HasSpatial == true)
                continue;

            if(ifb.Analyzer == null)
                ifb.SetAnalyzer(DefaultAnalyzer);
        }
        
        //We want also find maximum buffer for analyzers.
        UpdateMaximumOutputAndTokenSize();
    }

    internal void UpdateMaximumOutputAndTokenSize()
    {
        foreach (var analyzer in CollectionsMarshal.AsSpan(FieldsList))
        {
            if (analyzer.Analyzer == null)
                continue;
            
            _maximumOutputSize = Math.Max(_maximumOutputSize, analyzer.Analyzer.DefaultOutputSize);
            _maximumTokenSize = Math.Max(_maximumTokenSize, analyzer.Analyzer.DefaultTokenSize);
        }
    }
        
    
    public IndexFieldBinding GetByFieldId(int fieldId)
    {
        return FieldsById[fieldId];
    }

    public bool TryGetByFieldId(int fieldId, out IndexFieldBinding binding)
    {
        return FieldsById.TryGetValue(fieldId, out binding);
    }

    public IndexFieldBinding GetByFieldName(string fieldName)
    {
        // This method is a convenience method that should not be used in high performance sections of the code.
        using var _ = Slice.From(Context, fieldName, out var str);
        return Fields[str];
    }

    public bool TryGetByFieldName(string fieldName, out IndexFieldBinding binding)
    {
        // This method is a convenience method that should not be used in high performance sections of the code.
        using var _ = Slice.From(Context, fieldName, out var str);
        return Fields.TryGetValue(str, out binding);
    }

    protected void AddNewBinding(int fieldId, Slice fieldName, Analyzer analyzer = null, bool hasSuggestion = false, FieldIndexingMode fieldIndexingMode = FieldIndexingMode.Normal, bool hasSpatial = false)
    {
        if (FieldsById.TryGetValue(fieldId, out var storedAnalyzer) == false)
        {
            GetFieldNameForDoubles(Context, fieldName, out var fieldNameDouble);
            GetFieldNameForLongs(Context, fieldName, out var fieldNameLong);
            var binding = new IndexFieldBinding(fieldId, fieldName,  fieldNameLong,fieldNameDouble, 
                analyzer, hasSuggestion, fieldIndexingMode, hasSpatial);
            Fields[fieldName] = binding;
            FieldsById[fieldId] = binding;
            FieldsList.Add(binding);
        }
        else
        {
            Debug.Assert(analyzer == storedAnalyzer.Analyzer);
        }
    }

    public IndexFieldsMappingBase AddBinding(IndexFieldBinding binding)
    {
        var fieldId = binding.FieldId;
        var fieldName = binding.FieldName;
        
        var canAdd = FieldsById.ContainsKey(fieldId) == false && Fields.ContainsKey(fieldName) == false;

        if (canAdd)
        {
            Fields[fieldName] = binding;
            FieldsById[fieldId] = binding;
            FieldsList.Add(binding);
        }
        else
        {
            throw new InvalidDataException("Cannot add already existing item into index mapping.");
        }

        return this;
    }
    
    public IndexFieldBinding GetByFieldName(Slice fieldName)
    {
        return Fields[fieldName];
    }

    public bool TryGetByFieldName(Slice fieldName, out IndexFieldBinding binding)
    {
        return Fields.TryGetValue(fieldName, out binding);
    }

    public IEnumerator<IndexFieldBinding> GetEnumerator()
    {
        return FieldsList.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return FieldsList.GetEnumerator();
    }
}

public class IndexDynamicFieldsMapping : IndexFieldsMappingBase
{
    internal IndexDynamicFieldsMapping(ByteStringContext context, Func<string, Analyzer> searchAnalyzer, Analyzer defaultAnalyzer, Func<string, Analyzer> exactAnalyzer) : base(context, exactAnalyzer, searchAnalyzer)
    {
        DefaultAnalyzer = defaultAnalyzer;
    }

    public IndexDynamicFieldsMapping AddBinding(string fieldName, FieldIndexingMode fieldIndexingMode)
    {
        if (TryGetByFieldName(fieldName, out var binding))
            return this;
        
        Slice.From(Context, fieldName, ByteStringType.Immutable, out var str);
        
        var analyzer = fieldIndexingMode switch
        {
            FieldIndexingMode.Search => DefaultSearchAnalyzer(fieldName),
            FieldIndexingMode.Exact => DefaultExactAnalyzer(fieldName),
            FieldIndexingMode.Normal => DefaultAnalyzer,
            FieldIndexingMode.No => null,
            _ => throw new ArgumentOutOfRangeException(nameof(fieldIndexingMode), fieldIndexingMode, null)
        };

        AddNewBinding(Count + 1, str, analyzer, fieldIndexingMode: fieldIndexingMode);
        return this;
    }
}

public class IndexFieldsMapping : IndexFieldsMappingBase
{


    public IndexFieldsMapping(ByteStringContext context) : base(context, null, null)
    {
    }

    public IndexFieldsMapping(ByteStringContext context, Func<string, Analyzer> searchAnalyzer, Func<string, Analyzer> exactAnalyzer) : base(context, exactAnalyzer, searchAnalyzer)
    {
    } 
    
    public IndexFieldsMapping AddBinding(int fieldId, string fieldName)
    {
        Slice.From(Context, fieldName, ByteStringType.Immutable, out var str);
        AddBinding(fieldId, str);
        return this;
    }

    public IndexFieldsMapping AddBinding(int fieldId, Slice fieldName, Analyzer analyzer = null, bool hasSuggestion = false,
        FieldIndexingMode fieldIndexingMode = FieldIndexingMode.Normal, bool hasSpatial = false)
    {
        AddNewBinding(fieldId, fieldName, analyzer, hasSuggestion, fieldIndexingMode, hasSpatial);
        return this;
    }
    
    public IndexDynamicFieldsMapping CreateIndexMappingForDynamic()
    {
        if (DefaultExactAnalyzer == null || DefaultAnalyzer == null || DefaultSearchAnalyzer == null)
            throw new InvalidDataException($"Cannot create IndexMapping for dynamic fields because analyzers are not created...");

        return new IndexDynamicFieldsMapping(Context, DefaultSearchAnalyzer, DefaultAnalyzer, DefaultExactAnalyzer);
    }
}

public class IndexFieldBinding
{
    public readonly int FieldId;
    public readonly Slice FieldName;
    public readonly Slice FieldNameLong;
    public readonly Slice FieldNameDouble;
    public Analyzer Analyzer => _analyzer;
    private Analyzer _analyzer;
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
        HasSuggestions = hasSuggestions;
        _fieldIndexingMode = fieldIndexingMode;
        HasSpatial = hasSpatial;
        SetAnalyzer(analyzer);
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
        SetAnalyzer(_analyzer);// re-compute IsAnalyzed
    }

    public void SetAnalyzer(Analyzer analyzer)
    {
        _analyzer = analyzer;
        IsAnalyzed = _analyzer is not null && FieldIndexingMode is not FieldIndexingMode.Exact && HasSpatial is false;
    }
}
