using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Analyzers;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;

namespace Corax.Mappings;

public sealed class IndexFieldsMappingBuilder : IDisposable
{
    private readonly ByteStringContext _context;
    private const short LongSuffix = 19501; //"-L"
    private const short DoubleSuffix = 17453; //"-D"
    private const short TermTotalSumField = 17197; //"-C"
    
    private readonly Dictionary<Slice, IndexFieldBinding> _fields;
    private readonly Dictionary<int, IndexFieldBinding> _fieldsById;
    
    private bool _materialized;
    private readonly bool _isDynamic;
    
    private Func<string, Analyzer> _exactAnalyzer;
    private Func<string, Analyzer> _searchAnalyzer;
    private Analyzer _defaultAnalyzer;
    private readonly bool _isForWriter;
    private int _maximumOutputSize = Constants.Terms.MaxLength;
    private int _maximumTokenSize = Constants.Terms.MaxLength;

    public int Count => _fields.Count;

    public static IndexFieldsMappingBuilder CreateForWriter(bool isDynamic) => new IndexFieldsMappingBuilder(true, isDynamic);
    
    public static IndexFieldsMappingBuilder CreateForReader() => new IndexFieldsMappingBuilder(false, false);
    
    private IndexFieldsMappingBuilder(bool isForWriter, bool isDynamic = false)
    {
        _context = new ByteStringContext(SharedMultipleUseFlag.None);
        _fields = new(SliceComparer.Instance);
        _fieldsById = new();
        _isForWriter = isForWriter;
        _isDynamic = isDynamic;
    }

    public IndexFieldsMappingBuilder AddBindingToEnd(string fieldName, Analyzer analyzer = null, bool hasSuggestion = false,
        FieldIndexingMode fieldIndexingMode = FieldIndexingMode.Normal, bool shouldStore = false, bool hasSpatial = false)
    {
        Slice.From(_context, fieldName, out var slice);
        return AddBinding(_fields.Count, slice, analyzer, hasSuggestion, fieldIndexingMode, shouldStore, hasSpatial);
    }

    public IndexFieldsMappingBuilder AddBindingToEnd(Slice fieldName, Analyzer analyzer = null, bool hasSuggestion = false,
        FieldIndexingMode fieldIndexingMode = FieldIndexingMode.Normal, bool shouldStore = false,  bool hasSpatial = false) =>
        AddBinding(_fields.Count, fieldName, analyzer, hasSuggestion, fieldIndexingMode, shouldStore,  hasSpatial);

    public IndexFieldsMappingBuilder AddDynamicBinding(Slice fieldName, FieldIndexingMode mode, bool shouldStore)
    {
        if (_fields.TryGetValue(fieldName, out var storedBinding) == false)
        {
            AddBindingToEnd(fieldName, GetAnalyzer(), fieldIndexingMode: mode, shouldStore: shouldStore);
        }

        Analyzer GetAnalyzer() => mode switch
        {
            FieldIndexingMode.Search => _searchAnalyzer(fieldName.ToString()),
            FieldIndexingMode.Exact => _exactAnalyzer(fieldName.ToString()),
            FieldIndexingMode.No => null,
            _ => _defaultAnalyzer
        };

        return this;
    }
    
    public IndexFieldsMappingBuilder AddBinding(int fieldId, string fieldName, Analyzer analyzer = null, bool hasSuggestion = false,
        FieldIndexingMode fieldIndexingMode = FieldIndexingMode.Normal, bool shouldStore = false, bool hasSpatial = false)
    {
        Slice.From(_context, fieldName, out var slice);
        return AddBinding(fieldId, slice, analyzer, hasSuggestion, fieldIndexingMode, shouldStore , hasSpatial);
    }
    
    public IndexFieldsMappingBuilder AddBinding(int fieldId, Slice fieldName, Analyzer analyzer = null, bool hasSuggestion = false,
        FieldIndexingMode fieldIndexingMode = FieldIndexingMode.Normal, bool shouldStore = false, bool hasSpatial = false)
    {
        if (_fieldsById.TryGetValue(fieldId, out var storedAnalyzer) == false)
        {
            var clonedFieldName = fieldName.Clone(_context);
            GetFieldNameForDoubles(_context, clonedFieldName, out var fieldNameDouble);
            GetFieldNameForLongs(_context, clonedFieldName,  out var fieldNameLong);
            GetFieldForTotalSum(_context, clonedFieldName,  out var fieldForTotalSum);

            var binding = new IndexFieldBinding(fieldId, clonedFieldName, fieldNameLong, fieldNameDouble, fieldForTotalSum, _isForWriter,
                analyzer, hasSuggestion, fieldIndexingMode, shouldStore, hasSpatial);
            _fields[clonedFieldName] = binding;
            _fieldsById[fieldId] = binding;
        }
        else
        {
            Debug.Assert(analyzer == storedAnalyzer.Analyzer, $"Field '{fieldName}' no {fieldId} is already added but has different analyzer.");
        }

        return this;
    }

    internal static void GetFieldNameForLongs(ByteStringContext context, Slice fieldName, out Slice fieldNameForLongs)
    {
        GetFieldNameWithPostfix(context, fieldName, LongSuffix, out fieldNameForLongs);
    }

    internal static void GetFieldNameForDoubles(ByteStringContext context, Slice fieldName, out Slice fieldNameForDoubles)
    {
        GetFieldNameWithPostfix(context, fieldName, DoubleSuffix, out fieldNameForDoubles);
    }

    internal static void GetFieldForTotalSum(ByteStringContext context, Slice fieldName, out Slice fieldForTotalSum)
    {
        GetFieldNameWithPostfix(context, fieldName, TermTotalSumField, out fieldForTotalSum);
    }
    
    private static unsafe void GetFieldNameWithPostfix(ByteStringContext context, Slice fieldName, short postfix, out Slice fieldWithPostfix)
    {
        context.Allocate(fieldName.Size + sizeof(short), out ByteString output);
        fieldName.Content.CopyTo(output.Ptr);
        *(short*)(output.Ptr + fieldName.Size) = postfix;
        fieldWithPostfix = new Slice(SliceOptions.Key, output);
    }

    public IndexFieldsMappingBuilder AddSearchAnalyzer(Func<string, Analyzer> searchAnalyzer)
    {
        if (_searchAnalyzer != null)
            throw new InvalidOperationException($"Search analyzer is already added");

        _searchAnalyzer = searchAnalyzer;
        return this;
    }

    public IndexFieldsMappingBuilder AddExactAnalyzer(Func<string, Analyzer> exactAnalyzer)
    {
        if (_exactAnalyzer != null)
            throw new InvalidOperationException($"Exact analyzer is already added");

        _exactAnalyzer = exactAnalyzer;
        return this;
    }

    public IndexFieldsMappingBuilder AddDefaultAnalyzer(Analyzer defaultAnalyzer)
    {
        if (_defaultAnalyzer != null)
            throw new InvalidOperationException($"Default analyzer is already added");

        _defaultAnalyzer = defaultAnalyzer;
        return this;
    }
    
    public IndexFieldsMapping Build()
    {
        if (_materialized && (_isForWriter && _isDynamic) == false)
            throw new InvalidOperationException($"{nameof(IndexFieldsMappingBuilder)} is already materialized.");
        
        UpdateMaximumOutputAndTokenSize();
        var mapping = new IndexFieldsMapping(_context, _fields, _fieldsById, _defaultAnalyzer, _searchAnalyzer, _exactAnalyzer, _maximumOutputSize, _maximumTokenSize);
        _materialized = true;

        return mapping;
    }

    private void UpdateMaximumOutputAndTokenSize()
    {
        foreach (var (key, binding) in _fields)
        {
            if (binding.Analyzer == null)
                continue;

            _maximumOutputSize = Math.Max(_maximumOutputSize, binding.Analyzer.DefaultOutputSize);
            _maximumTokenSize = Math.Max(_maximumTokenSize, binding.Analyzer.DefaultTokenSize);
        }
    }

    public void Dispose()
    {
        if (_materialized == false)
            _context.Dispose();
    }
}
