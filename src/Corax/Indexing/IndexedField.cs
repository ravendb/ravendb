using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Corax.Analyzers;
using Corax.Mappings;
using Sparrow;
using Sparrow.Collections;
using Voron;
using Voron.Data.Graphs;
using Voron.Impl;
using Voron.Util;

namespace Corax.Indexing;

internal sealed class IndexedField
{
    private readonly IndexedField _parent;
    public struct SpatialEntry
    {
        public List<(double, double)> Locations;
        public int TermsPerEntryIndex;
    }

    public Dictionary<long, SpatialEntry> Spatial;
    public readonly FastList<EntriesModifications> Storage;
    public readonly Dictionary<Slice, int> Textual;
    public Hnsw.Registration VectorIndexWriter;

    /// <summary>
    /// Position matches position from _entryToTerms from IndexWriter which creates relation between entry and field
    /// </summary>
    private NativeList<NativeList<int>> _entryToTerms;
    public ref NativeList<NativeList<int>> EntryToTerms => ref _parent == null ? ref _entryToTerms : ref _parent._entryToTerms;
    public readonly Dictionary<long, int> Longs;
    public readonly Dictionary<double, int> Doubles;
    public Dictionary<Slice, int> Suggestions;
    public readonly Analyzer Analyzer;
    public readonly string NameForStatistics;
    public readonly Slice Name;
    public readonly Slice NameLong;
    public readonly Slice NameDouble;
    public readonly Slice NameTotalLengthOfTerms;
    public readonly int Id;
    public readonly FieldIndexingMode FieldIndexingMode;
    public readonly bool ShouldIndex;
    public readonly bool HasSuggestions;
    public readonly bool ShouldStore;
    private readonly SupportedFeatures _supportedFeatures;
    public readonly bool IsVirtual;
    public bool HasMultipleTermsPerField;
    public long FieldRootPage;
    public long TermsVectorFieldRootPage;
    public bool FieldSupportsPhraseQuery => _supportedFeatures.PhraseQuery && FieldIndexingMode is FieldIndexingMode.Search;

    public bool HasVector => _vectorOptions != null;
    private bool _hnswIsCreated = false;
    private readonly VectorOptions _vectorOptions;
    public override string ToString()
    {
        return Name.ToString() + " Id: " + Id;
    }

    public IndexedField(IndexFieldBinding binding, in SupportedFeatures supportedFeatures) : this(binding.FieldId, binding.FieldName, binding.FieldNameLong, binding.FieldNameDouble,
        binding.FieldTermTotalSumField, binding.Analyzer, binding.FieldIndexingMode, binding.HasSuggestions, binding.ShouldStore, supportedFeatures, binding.VectorOptions, binding.FieldNameForStatistics)
    {
    }

    private IndexedField(int id, Slice name, Slice nameLong, Slice nameDouble, Slice nameTotalLengthOfTerms, Analyzer analyzer,
        FieldIndexingMode fieldIndexingMode, bool hasSuggestions, bool shouldStore, in SupportedFeatures supportedFeatures, string nameForStatistics, long fieldRootPage, long termsVectorFieldRootPage, FastList<EntriesModifications> storage, Dictionary<Slice, int> textual, Dictionary<long, int> longs, Dictionary<double, int> doubles, IndexedField parent)
    {
        _parent = parent;
        Name = name;
        NameLong = nameLong;
        NameDouble = nameDouble;
        NameTotalLengthOfTerms = nameTotalLengthOfTerms;
        Id = id;
        Analyzer = analyzer;
        HasSuggestions = hasSuggestions;
        ShouldStore = shouldStore;
        _supportedFeatures = supportedFeatures;
        FieldRootPage = fieldRootPage;
        TermsVectorFieldRootPage = termsVectorFieldRootPage;
        Storage = storage;
        Textual = textual;
        Longs = longs;
        Doubles = doubles;
        FieldIndexingMode = fieldIndexingMode;
        ShouldIndex = supportedFeatures.StoreOnly == false || fieldIndexingMode != FieldIndexingMode.No;
        NameForStatistics = nameForStatistics ?? $"Field_{Name}";
        VectorIndexWriter = _parent.VectorIndexWriter;
        
        IsVirtual = true;
        if (fieldIndexingMode is FieldIndexingMode.Search && _parent.EntryToTerms.IsValid == false)
            EntryToTerms = new();
    }
    
    public IndexedField(int id, Slice name, Slice nameLong, Slice nameDouble, Slice nameTotalLengthOfTerms, Analyzer analyzer,
        FieldIndexingMode fieldIndexingMode, bool hasSuggestions, bool shouldStore, in SupportedFeatures supportedFeatures, VectorOptions vectorOptions, string nameForStatistics = null, long fieldRootPage = -1, long termsVectorFieldRootPage = -1)
    {
        Name = name;
        NameLong = nameLong;
        NameDouble = nameDouble;
        NameTotalLengthOfTerms = nameTotalLengthOfTerms;
        Id = id;
        Analyzer = analyzer;
        HasSuggestions = hasSuggestions;
        ShouldStore = shouldStore;
        _supportedFeatures = supportedFeatures;
        FieldRootPage = fieldRootPage;
        TermsVectorFieldRootPage = termsVectorFieldRootPage;
        Storage = new FastList<EntriesModifications>();
        Textual = new Dictionary<Slice, int>(SliceComparer.Instance);
        Longs = new Dictionary<long, int>();
        Doubles = new Dictionary<double, int>();
        FieldIndexingMode = fieldIndexingMode;
        ShouldIndex = supportedFeatures.StoreOnly == false || fieldIndexingMode != FieldIndexingMode.No;
        NameForStatistics = nameForStatistics ?? $"Field_{Name}";
        _vectorOptions = vectorOptions;
        if (fieldIndexingMode is FieldIndexingMode.Search)
            EntryToTerms = new();
    }

    public IndexedField CreateVirtualIndexedField(IndexFieldBinding dynamicField)
    {
        Analyzer analyzer;
        FieldIndexingMode fieldIndexingMode;
        //backward compatibility
        switch (dynamicField.FieldIndexingMode)
        {
            case FieldIndexingMode.No:
                analyzer = null;
                fieldIndexingMode = FieldIndexingMode.No;
                break;
            default:
                analyzer = Analyzer ?? dynamicField.Analyzer;
                fieldIndexingMode = Analyzer is null ? dynamicField.FieldIndexingMode : FieldIndexingMode;
                break;
        }
        
        return new IndexedField(Constants.IndexWriter.DynamicField, dynamicField.FieldName, dynamicField.FieldNameLong, dynamicField.FieldNameDouble,
            dynamicField.FieldTermTotalSumField, analyzer, fieldIndexingMode, dynamicField.HasSuggestions, dynamicField.ShouldStore,
            _supportedFeatures, dynamicField.FieldNameForStatistics, FieldRootPage, TermsVectorFieldRootPage, Storage, Textual, Longs, Doubles, this);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Hnsw.Registration GetVectorWriter(LowLevelTransaction llt, int vectorSize = Constants.IndexWriter.Hnsw.TreeExists)
    {
        if (_hnswIsCreated == false && vectorSize != Constants.IndexWriter.Hnsw.TreeExists)
            CreateHnswTree(llt, vectorSize);
        
        return VectorIndexWriter ??= Hnsw.RegistrationFor(llt, Name);
    }

    private void CreateHnswTree(LowLevelTransaction llt, int vectorSize)
    {
        PortableExceptions.ThrowIfNull<InvalidOperationException>(_vectorOptions, $"{nameof(_vectorOptions)} is null)");
        Hnsw.Create(llt, Name, vectorSize, _vectorOptions.NumberOfEdges, _vectorOptions.NumberOfCandidates);
    }
    
    public void Clear()
    {
        Suggestions?.Clear();
        Doubles?.Clear();
        Spatial?.Clear();
        Longs?.Clear();
        Textual?.Clear();
        EntryToTerms = default;
        
        PortableExceptions.ThrowIfOnDebug<InvalidOperationException>(VectorIndexWriter is { IsCommited: true }, "VectorIndexer is { IsDisposed: true }");
        VectorIndexWriter = null; // after Commit it will be recreated from scratch
    }
}
