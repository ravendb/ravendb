using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Server.Compression;
using Voron;
using Voron.Impl;
using Voron.Data.Containers;
using Sparrow;
using System.Runtime.Intrinsics.X86;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Queries;
using Corax.Queries.TermProviders;
using Corax.Utils;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Fixed;
using Voron.Data.Lookups;
using InvalidOperationException = System.InvalidOperationException;
using static Voron.Data.CompactTrees.CompactTree;

namespace Corax;

public sealed unsafe partial class IndexSearcher : IDisposable
{
    internal readonly Transaction _transaction;
    private Dictionary<string, Slice> _dynamicFieldNameMapping;

    private readonly IndexFieldsMapping _fieldMapping;
    private Tree _persistedDynamicTreeAnalyzer;
    private long? _numberOfEntries;

    /// <summary>
    /// When true no SIMD instruction will be used. Useful for checking that optimized algorithms behave in the same
    /// way than reference algorithms. 
    /// </summary>
    public bool ForceNonAccelerated { get; set; }

    public bool IsAccelerated => Avx2.IsSupported && !ForceNonAccelerated;

    public long NumberOfEntries => _numberOfEntries ??= _metadataTree?.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0;
    
    public ByteStringContext Allocator => _transaction.Allocator;

    internal Transaction Transaction => _transaction;

    public IndexFieldsMapping FieldMapping => _fieldMapping;

    private readonly bool _ownsTransaction;
    private readonly bool _ownsIndexMapping;

    private Tree _metadataTree;
    private Tree _fieldsTree;
    private Tree _entriesToTermsTree;
    private Tree _entriesToSpatialTree;
    private long _dictionaryId;
    private Lookup<Int64LookupKey> _entryIdToLocation;
    public FieldsCache FieldCache;

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
    
    public EntryTermsReader GetEntryTermsReader(long id, ref Page p)
    {
        if (_entryIdToLocation.TryGetValue(id, out var loc) == false)
            throw new InvalidOperationException("Unable to find entry id: " + id);
        var item = Container.MaybeGetFromSamePage(_transaction.LowLevelTransaction, ref p, loc);
        return new EntryTermsReader(_transaction.LowLevelTransaction, item.Address, item.Length, _dictionaryId);
    }


    internal Slice EncodeAndApplyAnalyzer(in FieldMetadata binding, ReadOnlySpan<char> term, bool canReturnEmptySlice = false)
    {
        if (term.Length == 0 || term.SequenceEqual(Constants.EmptyStringCharSpan.Span))
            return Constants.EmptyStringSlice;

        if (term.SequenceEqual(Constants.NullValueCharSpan.Span))
            return Constants.NullValueSlice;
        
        using var _ = Allocator.Allocate(Encodings.Utf8.GetByteCount(term), out Span<byte> termBuffer);
        var byteCount = Encodings.Utf8.GetBytes(term, termBuffer);
        
        ApplyAnalyzer(binding, termBuffer.Slice(0, byteCount), out var encodedTerm, canReturnEmptySlice);
        return encodedTerm;
    }

    //Function used to generate Slice from query parameters.
    //We cannot dispose them before the whole query is executed because they are an integral part of IQueryMatch.
    //We know that the Slices are automatically disposed when the transaction is closed so we don't need to track them.
    [SkipLocalsInit]
    internal Slice EncodeAndApplyAnalyzer(in FieldMetadata binding, string term, bool canReturnEmptySlice = false)
    {
        if (term is null)
            return default;

        if (term.Length == 0 || term == Constants.EmptyString)
            return Constants.EmptyStringSlice;

        if (term == Constants.NullValue)
            return Constants.NullValueSlice;

        ApplyAnalyzer(binding, Encodings.Utf8.GetBytes(term), out var encodedTerm, canReturnEmptySlice);
        return encodedTerm;
    }

    internal ByteStringContext<ByteStringMemoryCache>.InternalScope ApplyAnalyzer(string originalTerm, int fieldId, out Slice value, bool allowToBeEmpty = false)
    {
        using (Slice.From(Allocator, originalTerm, ByteStringType.Immutable, out var originalTermSliced))
        {
            return ApplyAnalyzer(originalTermSliced, fieldId, out value, allowToBeEmpty);
        }
    }

    internal ByteStringContext<ByteStringMemoryCache>.InternalScope ApplyAnalyzer(FieldMetadata binding, ReadOnlySpan<byte> originalTerm, out Slice value, bool allowToBeEmpty = false)
    {
        Analyzer analyzer = binding.Analyzer;
        if (binding.FieldId == Constants.IndexWriter.DynamicField && binding.Mode is not (FieldIndexingMode.Exact or FieldIndexingMode.No))
        {
            analyzer = _fieldMapping.DefaultAnalyzer;
        }
        else
        {
            if (binding.Mode is FieldIndexingMode.Exact || binding.Analyzer is null)
            {
                var disposable = Allocator.AllocateDirect(originalTerm.Length, ByteStringType.Mutable, out var originalTermSliced);
                originalTerm.CopyTo(new Span<byte>(originalTermSliced._pointer->Ptr, originalTerm.Length));

                value = new Slice(originalTermSliced);
                return disposable;
            }
        }

        return AnalyzeTerm(analyzer, originalTerm, out value, allowToBeEmpty);
    }

    internal ByteStringContext<ByteStringMemoryCache>.InternalScope ApplyAnalyzer(ReadOnlySpan<byte> originalTerm, int fieldId, out Slice value, bool allowToBeEmpty = false)
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

        return AnalyzeTerm(analyzer, originalTerm, out value, allowToBeEmpty);
    }

    [SkipLocalsInit]
    internal ByteStringContext<ByteStringMemoryCache>.InternalScope ApplyAnalyzer(Slice originalTerm, int fieldId, out Slice value, bool allowToBeEmpty = false)
    {
        return ApplyAnalyzer(originalTerm.AsSpan(), fieldId, out value, allowToBeEmpty);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ByteStringContext<ByteStringMemoryCache>.InternalScope AnalyzeTerm(Analyzer analyzer, ReadOnlySpan<byte> originalTerm, out Slice value, bool allowToBeEmpty = false)
    {
        analyzer.GetOutputBuffersSize(originalTerm.Length, out int outputSize, out int tokenSize);

        Debug.Assert(outputSize < 1024 * 1024, "Term size is too big for analyzer.");
        Debug.Assert(Unsafe.SizeOf<Token>() * tokenSize < 1024 * 1024, "Analyzer wants to create too much tokens.");

        var buffer = Analyzer.BufferPool.Rent(outputSize);
        var tokens = Analyzer.TokensPool.Rent(tokenSize);

        Span<byte> bufferSpan = buffer.AsSpan();
        Span<Token> tokensSpan = tokens.AsSpan();
        analyzer.Execute(originalTerm, ref bufferSpan, ref tokensSpan);
        if (allowToBeEmpty == false && tokensSpan.Length != 1) 
            Debug.Assert(tokensSpan.Length == 1, $"{nameof(ApplyAnalyzer)} should create only 1 token as a result.");
        var disposable = Slice.From(Allocator, bufferSpan, ByteStringType.Immutable, out value);

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
   
    public long GetTermAmountInField(FieldMetadata field)
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);

        return terms?.NumberOfEntries ?? 0;
    }

    public bool TryGetTermsOfField(FieldMetadata field, out ExistsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator> existsTermProvider)
    {
        return TryGetTermsOfField<Lookup<CompactKeyLookup>.ForwardIterator>(field, out existsTermProvider);
    }

    public bool TryGetTermsOfField<TLookupIterator>(FieldMetadata field, out ExistsTermProvider<TLookupIterator> existsTermProvider)
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

    public FieldMetadata GetWriterFieldMetadata(string fieldName)
    {
        var handler = Slice.From(Allocator, fieldName, ByteStringType.Immutable, out var fieldNameSlice);
        if (_fieldMapping.TryGetByFieldName(fieldNameSlice, out var binding))
        {
            handler.Dispose();
            return binding.Metadata;
        }

        var mode = GetFieldIndexingModeForDynamic(fieldNameSlice);

        return FieldMetadata.Build(fieldNameSlice, binding.FieldTermTotalSumField, Constants.IndexWriter.DynamicField, mode, mode switch
        {
            FieldIndexingMode.Search => _fieldMapping.SearchAnalyzer(fieldName),
            FieldIndexingMode.Exact => _fieldMapping.ExactAnalyzer(fieldName),
            FieldIndexingMode.Normal => _fieldMapping.DefaultAnalyzer,
            _ => null,
        });
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

    public bool HasMultipleTermsInField(in FieldMetadata fieldMetadata)
    {
        return HasMultipleTermsInField(fieldMetadata.FieldName);
    }
    
    private bool HasMultipleTermsInField(Slice fieldName)
    {
        if (_metadataTree is null)
            return false;
        
        using var it = _metadataTree.MultiRead(Constants.IndexWriter.MultipleTermsInField);
        return it.Seek(fieldName) && SliceComparer.Equals(it.CurrentKey, fieldName);
    }

    public Dictionary<long, string> GetIndexedFieldNamesByRootPage()
    {
        var pageToField = new Dictionary<long, string>();
        var it = _fieldsTree.Iterate(prefetch: false);
        if (it.Seek(Slices.BeforeAllKeys))
        {
            do
            {
                var state = (LookupState*)it.CreateReaderForCurrent().Base;
                if (state->RootObjectType == RootObjectType.Lookup)
                {
                    pageToField.Add(state->RootPage, it.CurrentKey.ToString());
                }
            } while (it.MoveNext());
        }

        return pageToField;
    }
}
