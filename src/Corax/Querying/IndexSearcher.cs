using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;
using System.Text;
using Corax.Analyzers;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.TermProviders;
using Corax.Utils;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Fixed;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Impl;
using InvalidOperationException = System.InvalidOperationException;
using static Voron.Data.CompactTrees.CompactTree;
using Voron.Util;
using System.Runtime.Intrinsics;

namespace Corax.Querying;

public sealed unsafe partial class IndexSearcher : IDisposable
{
    internal readonly Transaction _transaction;
    private Dictionary<string, Slice> _dynamicFieldNameMapping;

    private readonly IndexFieldsMapping _fieldMapping;
    private HashSet<long> _nullTermsMarkers;
    private HashSet<long> _nonExistingTermsMarkers;
    private Tree _persistedDynamicTreeAnalyzer;
    private long? _numberOfEntries;
    public bool _nullTermsMarkersLoaded;
    private bool _nonExistingTermsMarkersLoaded;

    /// <summary>
    /// When true no SIMD instruction will be used. Useful for checking that optimized algorithms behave in the same
    /// way than reference algorithms. 
    /// </summary>
    public bool ForceNonAccelerated { get; set; }

    public bool IsAccelerated => Vector256.IsHardwareAccelerated && !ForceNonAccelerated;

    public long NumberOfEntries => _numberOfEntries ??= _metadataTree?.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0;
    
    public ByteStringContext Allocator => _transaction.Allocator;

    internal Transaction Transaction => _transaction;

    private readonly bool _ownsTransaction;
    private readonly bool _ownsIndexMapping;

    private Tree _metadataTree;
    private Tree _multipleTermsInField;
    private Tree _fieldsTree;
    private Tree _entriesToTermsTree;
    private Tree _entriesToSpatialTree;
    private Tree _nullPostingListsTree;
    private Tree _nonExistingPostingListsTree;
    private long _dictionaryId;
    private Lookup<Int64LookupKey> _entryIdToLocation;
    public FieldsCache FieldCache;
    private bool _nullPostingListsTreeLoaded;
    private bool _nonExistingPostingListsTreeLoaded;

    public long MaxMemoizationSizeInBytes = 128 * 1024 * 1024;

    public bool DocumentsAreBoosted => GetDocumentBoostTree().NumberOfEntries > 0;

    
    // The reason why we want to have the transaction open for us is so that we avoid having
    // to explicitly provide the index searcher with opening semantics and also every new
    // searcher becomes essentially a unit of work which makes reusing assets tracking more explicit.
    public IndexSearcher(StorageEnvironment environment, IndexFieldsMapping fieldsMapping) : this(fieldsMapping)
    {
        _ownsTransaction = true;
        _transaction = environment.ReadTransaction();
        Init();
    }

    public IndexSearcher(Transaction tx, IndexFieldsMapping fieldsMapping) : this(fieldsMapping)
    {
        _ownsTransaction = false;
        _transaction = tx;
        Init();
    }

    private void Init()
    {
        _fieldsTree = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
        _entriesToTermsTree = _transaction.ReadTree(Constants.IndexWriter.EntriesToTermsSlice);
        _metadataTree = _transaction.ReadTree(Constants.IndexMetadataSlice);
        _multipleTermsInField = _transaction.ReadTree(Constants.IndexWriter.MultipleTermsInField);
        _entryIdToLocation = _transaction.LookupFor<Int64LookupKey>(Constants.IndexWriter.EntryIdToLocationSlice);
        _dictionaryId = CompactTree.GetDictionaryId(_transaction.LowLevelTransaction);
        FieldCache = new FieldsCache(_transaction, _fieldsTree);
    }

    private IndexSearcher(IndexFieldsMapping fieldsMapping)
    {
        if (fieldsMapping is null)
        {
            _ownsIndexMapping = true;
            using var builder = IndexFieldsMappingBuilder.CreateForReader();
            _fieldMapping = builder.Build();
        }
        else
        {
            _fieldMapping = fieldsMapping;
        }
    }
    
    public EntryTermsReaderEnumerator GetEntryTermsReader(Span<long> ids, PageLocator pageLocator)
    {
        InitializeSpecialTermsMarkers();

        return new EntryTermsReaderEnumerator(this, ids, pageLocator);
    }

    public ref struct EntryTermsReaderEnumerator
    {
        private readonly IndexSearcher _searcher;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _scope;
        private readonly Span<UnmanagedSpan> _spans;
        private int _index;

        public EntryTermsReaderEnumerator(IndexSearcher searcher,Span<long> ids, PageLocator pageLocator)
        {
            _searcher = searcher;
            _scope = searcher._transaction.Allocator.AllocateDirect(sizeof(UnmanagedSpan) * ids.Length, out var spanBuffer);
            _spans = spanBuffer.ToSpan<UnmanagedSpan>();
            
            using var _ = searcher._transaction.Allocator.AllocateDirect(sizeof(long) * ids.Length, out var locationsBuffer);
            var locations = locationsBuffer.ToSpan<long>();
            searcher._entryIdToLocation.GetFor(ids, locations, -1);
            Container.GetAll(searcher._transaction.LowLevelTransaction, locations, (UnmanagedSpan*)spanBuffer.Ptr, -1, pageLocator);
            _index = -1;
            Current = new(searcher._transaction.LowLevelTransaction, searcher._nullTermsMarkers, searcher._nonExistingTermsMarkers, searcher._dictionaryId);
        }

        public bool MoveNext()
        {
            if (++_index >= _spans.Length)
                return false;
            
            Current.Reuse(_spans[_index].Address, _spans[_index].Length);
            return true;
        }

        public EntryTermsReader Current;

        public void Dispose()
        {
            Current.Reset();
            _scope.Dispose();
        }
    } 
    
    public EntryTermsReader GetEntryTermsReader(long id, ref Page p)
    {
        if (_entryIdToLocation.TryGetValue(id, out var loc) == false)
            throw new InvalidOperationException("Unable to find entry id: " + id);

        InitializeSpecialTermsMarkers();
        
        var item = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref p, loc);
        return new EntryTermsReader(_transaction.LowLevelTransaction, _nullTermsMarkers, _nonExistingTermsMarkers, item.Address, item.Length, _dictionaryId);
    }
    
    internal void EncodeAndApplyAnalyzerForMultipleTerms(in FieldMetadata binding, ReadOnlySpan<char> term, ref ContextBoundNativeList<Slice> terms)
    {
        if (term.Length == 0 || term.SequenceEqual(Constants.EmptyStringCharSpan.Span))
        {
            terms.Add(Constants.EmptyStringSlice);
            return;
        }

        if (term.SequenceEqual(Constants.NullValueCharSpan.Span))
        {
            terms.Add(Constants.NullValueSlice);
            return;
        }

        using var _ = Allocator.Allocate(Encodings.Utf8.GetByteCount(term), out Span<byte> termBuffer);
        var byteCount = Encodings.Utf8.GetBytes(term, termBuffer);

        ApplyAnalyzerMultiTerms(binding, termBuffer[..byteCount], ref terms);
    }

    internal void ApplyAnalyzerMultiTerms(in FieldMetadata binding, ReadOnlySpan<byte> originalTerm, ref ContextBoundNativeList<Slice> terms)
    {
        Analyzer analyzer = binding.Analyzer;
        if (binding.FieldId == Constants.IndexWriter.DynamicField && binding.Mode is not (FieldIndexingMode.Exact or FieldIndexingMode.No))
        {
            analyzer = _fieldMapping.DefaultAnalyzer;
        }
        else if (binding.Mode is FieldIndexingMode.Exact || analyzer is null)
        {
            Allocator.AllocateDirect(originalTerm.Length, ByteStringType.Mutable, out var originalTermSliced);
            originalTerm.CopyTo(originalTermSliced.ToSpan());
            terms.Add(new Slice(originalTermSliced)); 
            return;
        }

        AnalyzeMultipleTerms(analyzer, originalTerm, ref terms);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AnalyzeMultipleTerms(Analyzer analyzer, ReadOnlySpan<byte> originalTerm, ref ContextBoundNativeList<Slice> terms)
    {
        analyzer.GetOutputBuffersSize(originalTerm.Length, out int outputSize, out int tokenSize);

        Debug.Assert(outputSize < 1024 * 1024, "Term size is too big for analyzer.");
        Debug.Assert(Unsafe.SizeOf<Token>() * tokenSize < 1024 * 1024, "Analyzer wants to create too much tokens.");

        var buffer = Analyzer.BufferPool.Rent(outputSize);
        var tokens = Analyzer.TokensPool.Rent(tokenSize);

        Span<byte> bufferSpan = buffer.AsSpan();
        Span<Token> tokensSpan = tokens.AsSpan();
        analyzer.Execute(originalTerm, ref bufferSpan, ref tokensSpan);
        for (int i = 0; i < tokensSpan.Length; i++)
        {
            var token = bufferSpan.Slice(tokensSpan[i].Offset, (int)tokensSpan[i].Length);
            _ = Indexing.IndexWriter.CreateNormalizedTerm(Allocator, token, out var value);
            terms.Add(value);
        }

        Analyzer.TokensPool.Return(tokens);
        Analyzer.BufferPool.Return(buffer);
    }
    internal Slice EncodeAndApplyAnalyzer(in FieldMetadata binding, Analyzer analyzer, ReadOnlySpan<char> term)
    {
        if (term.Length == 0 || term.SequenceEqual(Constants.EmptyStringCharSpan.Span))
            return Constants.EmptyStringSlice;

        if (term.SequenceEqual(Constants.NullValueCharSpan.Span))
            return Constants.NullValueSlice;
        
        using var _ = Allocator.Allocate(Encodings.Utf8.GetByteCount(term), out Span<byte> termBuffer);
        var byteCount = Encodings.Utf8.GetBytes(term, termBuffer);
        
        ApplyAnalyzer(binding, analyzer, termBuffer.Slice(0, byteCount), out var encodedTerm);
        return encodedTerm;
    }

    //Function used to generate Slice from query parameters.
    //We cannot dispose them before the whole query is executed because they are an integral part of IQueryMatch.
    //We know that the Slices are automatically disposed when the transaction is closed so we don't need to track them.
    [SkipLocalsInit]
    public Slice EncodeAndApplyAnalyzer(in FieldMetadata binding, string term)
    {
        if (term is null)
            return Constants.NullValueSlice; // unary match

        if (ReferenceEquals(term, Constants.BeforeAllKeys))
            return Slices.BeforeAllKeys;
        
        if (ReferenceEquals(term, Constants.AfterAllKeys))
            return Slices.AfterAllKeys;

        if (term.Length == 0 || term == Constants.EmptyString)
            return Constants.EmptyStringSlice;

        if (term == Constants.NullValue)
            return Constants.NullValueSlice;

        ApplyAnalyzer(binding, binding.Analyzer, Encodings.Utf8.GetBytes(term), out var encodedTerm);
        return encodedTerm;
    }

    public void ApplyAnalyzer(string originalTerm, int fieldId, out Slice value)
    {
        using (Slice.From(Allocator, originalTerm, ByteStringType.Immutable, out var originalTermSliced))
        {
            ApplyAnalyzer(originalTermSliced, fieldId, out value);
        }
    }

    public void ApplyAnalyzer(in FieldMetadata binding, Analyzer analyzer, ReadOnlySpan<byte> originalTerm, out Slice value)
    {
        if (binding.FieldId == Constants.IndexWriter.DynamicField && binding.Mode is not (FieldIndexingMode.Exact or FieldIndexingMode.No))
        {
            analyzer = _fieldMapping.DefaultAnalyzer;
        }
        else
        {
            if (binding.Mode is FieldIndexingMode.Exact || binding.Analyzer is null)
            {
                _ = Allocator.AllocateDirect(originalTerm.Length, ByteStringType.Mutable, out var originalTermSliced);
                originalTerm.CopyTo(new Span<byte>(originalTermSliced._pointer->Ptr, originalTerm.Length));

                value = new Slice(originalTermSliced);
                return;
            }
        }

        AnalyzeTerm(analyzer, originalTerm, out value);
    }

    internal ByteStringContext<ByteStringMemoryCache>.InternalScope ApplyAnalyzer(ReadOnlySpan<byte> originalTerm, int fieldId, out Slice value)
    {
        Analyzer analyzer = null;
        IndexFieldBinding binding = null;

        if (fieldId == Constants.IndexWriter.DynamicField)
        {
            analyzer = _fieldMapping.DefaultAnalyzer;
        }
        else if (_fieldMapping.TryGetByFieldId(fieldId, out binding) == false
                 || binding.FieldIndexingMode is FieldIndexingMode.Exact
                 || binding.Analyzer is null)
        {
            var disposable = Allocator.AllocateDirect(originalTerm.Length, ByteStringType.Mutable, out var originalTermSliced);
            originalTerm.CopyTo(new Span<byte>(originalTermSliced.Ptr, originalTerm.Length));

            value = new Slice(originalTermSliced);
            return disposable;
        }

        analyzer ??= binding.FieldIndexingMode is FieldIndexingMode.Search
            ? Analyzer.CreateLowercaseAnalyzer(this.Allocator) // lowercase only when search is used in non-full-text-search match 
            : binding.Analyzer!;

        return AnalyzeTerm(analyzer, originalTerm, out value);
    }

    [SkipLocalsInit]
    internal ByteStringContext<ByteStringMemoryCache>.InternalScope ApplyAnalyzer(Slice originalTerm, int fieldId, out Slice value)
    {
        return ApplyAnalyzer(originalTerm.AsSpan(), fieldId, out value);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ByteStringContext<ByteStringMemoryCache>.InternalScope AnalyzeTerm(Analyzer analyzer, ReadOnlySpan<byte> originalTerm, out Slice value)
    {
        analyzer.GetOutputBuffersSize(originalTerm.Length, out int outputSize, out int tokenSize);

        Debug.Assert(outputSize < 1024 * 1024, "Term size is too big for analyzer.");
        Debug.Assert(Unsafe.SizeOf<Token>() * tokenSize < 1024 * 1024, "Analyzer wants to create too much tokens.");

        var buffer = Analyzer.BufferPool.Rent(outputSize);
        var tokens = Analyzer.TokensPool.Rent(tokenSize);

        Span<byte> bufferSpan = buffer.AsSpan();
        Span<Token> tokensSpan = tokens.AsSpan();
        analyzer.Execute(originalTerm, ref bufferSpan, ref tokensSpan);
        if (tokensSpan.Length != 1)
            throw new NotSupportedException($"Analyzer turned term: {Encoding.UTF8.GetString(originalTerm)} into multiple terms ({tokensSpan.Length}), which is not allowed in this case.");
        var disposable = Indexing.IndexWriter.CreateNormalizedTerm(Allocator, bufferSpan, out value);

        Analyzer.TokensPool.Return(tokens);
        Analyzer.BufferPool.Return(buffer);

        return disposable;
    }
    
    public AllEntriesMatch AllEntries() => new AllEntriesMatch(this, _transaction);
   public TermMatch EmptyMatch() => TermMatch.CreateEmpty(this, Allocator);

   public long GetDictionaryIdFor(Slice field)
   {
       var terms = _fieldsTree?.CompactTreeFor(field);
       return terms?.DictionaryId ?? -1;
   }
   
    public long GetTermAmountInField(in FieldMetadata field)
    {
        long termAmount = 0;
        
        var fieldTree = _fieldsTree?.CompactTreeFor(field.FieldName);

        termAmount += fieldTree?.NumberOfEntries ?? 0;

        if (TryGetPostingListForNull(field, out var nullPostingListId))
        {
            var nullPostingList = GetPostingList(nullPostingListId);

            termAmount += nullPostingList?.State.NumberOfEntries ?? 0;
        }
        
        return termAmount;
    }

    public bool TryGetTermsOfField(in FieldMetadata field, out ExistsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator> existsTermProvider)
    {
        return TryGetTermsOfField<Lookup<CompactKeyLookup>.ForwardIterator>(field, out existsTermProvider);
    }

    public bool TryGetTermsOfField<TLookupIterator>(in FieldMetadata field, out ExistsTermProvider<TLookupIterator> existsTermProvider)
        where TLookupIterator : struct, ILookupIterator
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);

        if (terms == null)
        {
            existsTermProvider = default;
            return false;
        }

        existsTermProvider = new ExistsTermProvider<TLookupIterator>(this, terms, field);
        return true;
    }

    public List<string> GetFields()
    {
        List<string> fieldsInIndex = new();
        var fields = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);

        //Return an empty set when the index doesn't contain any fields. Case when index has 0 entries.
        if (fields is null)
            return fieldsInIndex;


        using (var it = fields.Iterate(false))
        {
            if (it.Seek(Slices.BeforeAllKeys))
            {
                do
                {
                    if (it.CurrentKey.EndsWith(Constants.IndexWriter.DoubleTreeSuffix) || it.CurrentKey.EndsWith(Constants.IndexWriter.LongTreeSuffix))
                        continue;

                    fieldsInIndex.Add(it.CurrentKey.ToString());
                } while (it.MoveNext());
            }
        }

        return fieldsInIndex;
    }

    public FieldIndexingMode GetFieldIndexingModeForDynamic(Slice name)
    {
        _persistedDynamicTreeAnalyzer ??= _transaction.ReadTree(Constants.IndexWriter.DynamicFieldsAnalyzersSlice);
        var readResult = _persistedDynamicTreeAnalyzer?.Read(name);
        if (readResult == null)
            return FieldIndexingMode.Normal;

        var mode = (FieldIndexingMode)readResult.Reader.ReadByte();
        return mode;
    }

    public FieldMetadata GetFieldMetadata(string fieldName, FieldIndexingMode mode = FieldIndexingMode.Normal)
    {
        var handler = Slice.From(Allocator, fieldName, ByteStringType.Immutable, out var fieldNameSlice);
        if (_fieldMapping.TryGetByFieldName(fieldNameSlice, out var binding))
        {
            handler.Dispose();
            return binding.Metadata;
        }
        
        Slice.From(Allocator, $"{fieldName}-C", ByteStringType.Immutable, out var fieldTermTotalSumField);

        return FieldMetadata.Build(fieldNameSlice, fieldTermTotalSumField, Constants.IndexWriter.DynamicField, mode, mode switch
        {
            FieldIndexingMode.Search => _fieldMapping.SearchAnalyzer(fieldName),
            FieldIndexingMode.Exact => _fieldMapping.ExactAnalyzer(fieldName),
            FieldIndexingMode.Normal => _fieldMapping.DefaultAnalyzer,
            _ => null,
        });
    }

    internal FixedSizeTree GetDocumentBoostTree()
    {
        return _transaction.FixedTreeFor(Constants.DocumentBoostSlice, sizeof(float));
    }


    public FieldMetadata FieldMetadataBuilder(string fieldName, int fieldId = Constants.IndexSearcher.NonAnalyzer, Analyzer analyzer = null,
        FieldIndexingMode fieldIndexingMode = default, bool hasBoost = false)
    {
        Slice.From(Allocator, fieldName, ByteStringType.Immutable, out var fieldNameAsSlice);
        Slice sumName = default;
        if (hasBoost)
            Slice.From(Allocator, $"{fieldName}-C", ByteStringType.Immutable, out sumName);
        
        return FieldMetadata.Build(fieldNameAsSlice, sumName, fieldId, fieldIndexingMode, analyzer, hasBoost);
    }

    public Slice GetDynamicFieldName(string fieldName)
    {
        _dynamicFieldNameMapping ??= new();
        if (_dynamicFieldNameMapping.TryGetValue(fieldName, out var sliceFieldName) == false)
        {
            Slice.From(Allocator, fieldName, ByteStringType.Immutable, out sliceFieldName);
            _dynamicFieldNameMapping.Add(fieldName, sliceFieldName);
        }

        return sliceFieldName;
    }

    public TermsReader TermsReaderFor(string name)
    {
        using (Slice.From(Allocator, name, ByteStringType.Immutable, out var nameSlice))
        {
            return TermsReaderFor(nameSlice);
        }
    }
    
    public TermsReader TermsReaderFor(Slice name)
    {
        if (_entriesToTermsTree == null)
            return default;
        return new TermsReader(_transaction.LowLevelTransaction, _entriesToTermsTree, name);
    }
    
    public SpatialReader SpatialReader(Slice name)
    {
        _entriesToSpatialTree ??= _transaction.ReadTree(Constants.IndexWriter.EntriesToSpatialSlice);

        if (_entriesToSpatialTree == null)
            return default;
        
        return new SpatialReader(_transaction.LowLevelTransaction, _entriesToSpatialTree, name);
    }
 
    public Lookup<Int64LookupKey> EntriesToTermsReader(Slice name)
    {
        return _entriesToTermsTree?.LookupFor<Int64LookupKey>(name);
    }

    
    public void Dispose()
    {
        if (_ownsTransaction)
            _transaction?.Dispose();

        if (_ownsIndexMapping)
            _fieldMapping?.Dispose();
    }

    // this is meant for debugging / tests only
    public Slice GetFirstIndexedFiledName() => _fieldMapping.GetFirstField().FieldName;

    public bool HasMultipleTermsInField(string fieldName)
    {
        using var _ = Slice.From(Allocator, fieldName, out var slice);
        return HasMultipleTermsInField(slice);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool HasMultipleTermsInField(in FieldMetadata fieldMetadata)
    {
        return HasMultipleTermsInField(fieldMetadata.FieldName);
    }

    //TODO PERFORMANCE
    private Dictionary<Slice, bool> _hasMultipleTermsInFieldCache;
    private bool HasMultipleTermsInField(Slice fieldName)
    {
        if (_multipleTermsInField is null)
            return false;

        _hasMultipleTermsInFieldCache ??= new(SliceComparer.Instance);

        ref var field = ref CollectionsMarshal.GetValueRefOrAddDefault(_hasMultipleTermsInFieldCache, fieldName, out bool exists);
        
        if (exists)
            return field;

        exists = _multipleTermsInField.Exists(fieldName);
        _hasMultipleTermsInFieldCache[fieldName] = exists;
        
        return exists;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetPostingListForNonExisting(in FieldMetadata field, out long postingListId) => TryGetPostingListForNonExisting(field.FieldName, out postingListId);
    
    private bool TryGetPostingListForNonExisting(Slice name, out long postingListId)
    {
        InitNonExistingPostingList();
        var result = _nonExistingPostingListsTree?.ReadStructure<(long PostingListId, long TermContainerId)>(name);
        if (result == null)
        {
            postingListId = -1;
            return false;
        }
        postingListId = result.Value.PostingListId;
        return true;
    }
    
    private void InitNonExistingPostingList()
    {
        if (_nonExistingPostingListsTreeLoaded == false)
        {
            _nonExistingPostingListsTreeLoaded = true;
            _nonExistingPostingListsTree = _transaction.ReadTree(Constants.IndexWriter.NonExistingPostingLists);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetPostingListForNull(in FieldMetadata field, out long postingListId) => TryGetPostingListForNull(field.FieldName, out postingListId);
    
    private bool TryGetPostingListForNull(Slice name, out long postingListId)
    {
        InitNullPostingList();
        var result = _nullPostingListsTree?.ReadStructure<(long PostingListId, long TermContainerId)>(name);
        if (result == null)
        {
            postingListId = -1;
            return false;
        }
        postingListId = result.Value.PostingListId;
        return true;
    }

    private void InitNullPostingList()
    {
        if (_nullPostingListsTreeLoaded == false)
        {
            _nullPostingListsTreeLoaded = true;
            _nullPostingListsTree = _transaction.ReadTree(Constants.IndexWriter.NullPostingLists);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IncludeNullMatch<TInner> IncludeNullMatch<TInner>(in FieldMetadata field, in TInner inner, bool forward)
        where TInner : IQueryMatch
    {
        return new IncludeNullMatch<TInner>(this, inner, field, forward);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IncludeNonExistingMatch<TInner> IncludeNonExistingMatch<TInner>(in FieldMetadata field, in TInner inner, bool forward)
        where TInner : IQueryMatch
    {
        return new IncludeNonExistingMatch<TInner>(this, inner, field, forward);
    }
    
    private void InitializeSpecialTermsMarkers()
    {
        if (_nullTermsMarkersLoaded == false)
        {
            _nullTermsMarkersLoaded = true;
            _nullTermsMarkers = new HashSet<long>();
            
            InitNullPostingList();
            
            if (_nullPostingListsTree != null)
                LoadSpecialTermMarkers(_nullPostingListsTree, _nullTermsMarkers);
        }

        if (_nonExistingTermsMarkersLoaded == false)
        {
            _nonExistingTermsMarkersLoaded = true;
            _nonExistingTermsMarkers = new HashSet<long>();
            
            InitNonExistingPostingList();
            
            if (_nonExistingPostingListsTree != null)
                LoadSpecialTermMarkers(_nonExistingPostingListsTree, _nonExistingTermsMarkers);
        }
    }

    public static void LoadSpecialTermMarkers(Tree postingList, HashSet<long> termsMarkers)
    {
        using (var it = postingList.Iterate(prefetch: false))
        {
            if (it.Seek(Slices.BeforeAllKeys))
            {
                do
                {
                    (_, long termId) = it.CreateReaderForCurrent().ReadStructure<(long, long)>();
                    termsMarkers.Add(termId);
                } while (it.MoveNext());
            }
        }
    }

    private long GetRootPageByFieldName(Slice fieldName)
    {
        var result = _fieldsTree?.Read(fieldName);
        if (result is null)
            return -1;
        
        var state = (LookupState*)result.Reader.Base;
        Debug.Assert(state->RootObjectType is RootObjectType.Lookup, "state->RootObjectType is RootObjectType.Lookup");
        return state->RootPage;
    }
    
    
    private Dictionary<long, Slice> _pageToField;
    public Dictionary<long, Slice> GetIndexedFieldNamesByRootPage()
    {
        if (_pageToField != null) return _pageToField;
        var pageToField = new Dictionary<long, Slice>();
        var it = _fieldsTree.Iterate(prefetch: false);
        if (it.Seek(Slices.BeforeAllKeys))
        {
            do
            {
                var state = (LookupState*)it.CreateReaderForCurrent().Base;
                if (state->RootObjectType == RootObjectType.Lookup)
                {
                    pageToField.Add(state->RootPage, it.CurrentKey.Clone(Allocator));
                }
            } while (it.MoveNext());
        }

        _pageToField = pageToField;
        return _pageToField;
    }
}
