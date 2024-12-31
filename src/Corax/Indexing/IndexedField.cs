using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Corax.Analyzers;
using Corax.Mappings;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Server;
using Voron;
using Voron.Data.Graphs;
using Voron.Impl;
using Voron.Util;
using VectorOptions = Corax.Mappings.VectorOptions;

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
    public Hnsw.Registration VectorIndexer;

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
    private long _fieldRootPage;

    public long FieldRootPage
    {
        get => _fieldRootPage;
   
        // The parent may still be uninitialized while initializing its virtual clone.
        // In such cases, we need to push the value to the parent.
        set
        {
            _fieldRootPage = value;
            if (IsVirtual && _parent.FieldRootPage == Constants.IndexWriter.UninitializedFieldRootPage)
                _parent.FieldRootPage = value;
        }
    }

    /// <summary>
    /// Root page of the TermsVectorField (used for phrase queries)
    /// </summary>
    public long TermsVectorFieldRootPage;
    public bool FieldSupportsPhraseQuery => _supportedFeatures.PhraseQuery && FieldIndexingMode is FieldIndexingMode.Search;
    public bool HasVector => _vectorOptions != null;

    private bool _hnswIsCreated;
    private VectorOptions _vectorOptions;

    public int GetApproximateNumberOfTerms()
    {
        var min = int.MaxValue;
        min = Math.Min(min, Textual.Count);
        if (VectorIndexer != null)
            min = Math.Min(min, VectorIndexer.AmountOfModifiedVectorsInTransaction);

        return min;
    }
    
    public override string ToString()
    {
        return Name.ToString() + " Id: " + Id;
    }

    public IndexedField(IndexFieldBinding binding, in SupportedFeatures supportedFeatures) : this(binding.FieldId, binding.FieldName, binding.FieldNameLong, binding.FieldNameDouble,
        binding.FieldTermTotalSumField, binding.Analyzer, binding.FieldIndexingMode, binding.HasSuggestions, binding.ShouldStore, supportedFeatures, binding.VectorOptions, binding.FieldNameForStatistics)
    {
    }

    private IndexedField(int id, Slice name, Slice nameLong, Slice nameDouble, Slice nameTotalLengthOfTerms, Analyzer analyzer,
        FieldIndexingMode fieldIndexingMode, bool hasSuggestions, bool shouldStore, in SupportedFeatures supportedFeatures, string nameForStatistics, long fieldRootPage, long termsVectorFieldRootPage, FastList<EntriesModifications> storage, Dictionary<Slice, int> textual, Dictionary<long, int> longs, Dictionary<double, int> doubles, VectorOptions vectorOptions, IndexedField parent)
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

        VectorIndexer = _parent.VectorIndexer;
        _vectorOptions = vectorOptions;
        IsVirtual = true;
        if (fieldIndexingMode is FieldIndexingMode.Search && _parent.EntryToTerms.IsValid == false)
            EntryToTerms = new();
    }
    
    public IndexedField(int id, Slice name, Slice nameLong, Slice nameDouble, Slice nameTotalLengthOfTerms, Analyzer analyzer,
        FieldIndexingMode fieldIndexingMode, bool hasSuggestions, bool shouldStore, in SupportedFeatures supportedFeatures, VectorOptions vectorOptions, string nameForStatistics = null, long fieldRootPage = Constants.IndexWriter.UninitializedFieldRootPage, long termsVectorFieldRootPage = Constants.IndexWriter.UninitializedFieldRootPage)
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
            _supportedFeatures, dynamicField.FieldNameForStatistics, FieldRootPage, TermsVectorFieldRootPage, Storage, Textual, Longs, Doubles, dynamicField.VectorOptions, this);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Hnsw.Registration GetVectorIndexer(LowLevelTransaction llt, int vectorSize = Constants.IndexWriter.Hnsw.TreeExists)
    {
        if (_hnswIsCreated == false && vectorSize != Constants.IndexWriter.Hnsw.TreeExists)
            CreateHnswTree(llt, vectorSize);

        if (VectorIndexer == null)
        {
            VectorIndexer = Hnsw.RegistrationFor(llt, Name);
            if (IsVirtual)
            {
                _parent._hnswIsCreated = true;
                _parent.VectorIndexer = VectorIndexer;
                _parent._vectorOptions = _vectorOptions;
            }
        }
        
        return VectorIndexer;
    }

    private void CreateHnswTree(LowLevelTransaction llt, int vectorSize)
    {
        PortableExceptions.ThrowIfNull<InvalidOperationException>(_vectorOptions, $"{nameof(_vectorOptions)} is null)");
        Hnsw.Create(llt, Name, vectorSize, _vectorOptions.NumberOfEdges, _vectorOptions.NumberOfCandidates, _vectorOptions.VectorEmbeddingType);
        _hnswIsCreated = true;
    }
    
    public void Clear()
    {
        Suggestions?.Clear();
        Doubles?.Clear();
        Spatial?.Clear();
        Longs?.Clear();
        Textual?.Clear();
        EntryToTerms = default;
        
        PortableExceptions.ThrowIfOnDebug<InvalidOperationException>(VectorIndexer is { IsCommited: false }, "VectorIndexer is { IsDisposed: false }");
        VectorIndexer = null; // after Commit it will be recreated from scratch
    }
}
