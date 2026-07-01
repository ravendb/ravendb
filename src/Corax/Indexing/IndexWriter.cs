using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Corax.Analyzers;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Utils.VxSort;
using Sparrow.Threading;
using Voron;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Fixed;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;


namespace Corax.Indexing
{
    public unsafe partial class IndexWriter : IDisposable // single threaded, controlled by caller
    {
        public long EntriesAllocatorTotalAllocationsInBytes => _entriesAllocator?._totalAllocated ?? 0;
        private readonly IndexEntryBuilder _entryBuilder;
        private long _numberOfModifications;
        private readonly HashSet<Slice> _indexedEntries = new(SliceComparer.Instance);
        private readonly IndexFieldsMapping _fieldsMapping;
        private readonly SupportedFeatures _supportedFeatures;

        // Structures used for document boosting. BoostedDocs is an in-memory cache for all documents that have been boosted during indexing.
        // DocumentBoost is a fixed tree that contains persisted boost values.
        private FixedSizeTree _documentBoost;
        private List<(DocumentEntryId EntryId, float Boost)> _boostedDocs;

        private Tree _indexMetadata;
        private long _numberOfTermModifications;

        private long[] _persistedVectorRootPages;
        private HashSet<long> _newVectorRootPages;

        private CompactKeyCacheScope _compactKeyScope;

        /// <summary>
        /// Meta-tree that contains mapping Field -> EntryId -> TermId.
        /// This is used by SortingMatches for building an array of terms to sort.
        /// </summary>
        private Tree _entriesToTermsTree;

        private ContextBoundNativeList<long> _smallPostingListWorkingBuffer;

        // For testing purposes only. 
        private bool _ownsTransaction;

        private JsonOperationContext _jsonOperationContext;
        private readonly Transaction _transaction;


        private bool _hasSuggestions;
        private readonly IndexedField[] _knownFieldsTerms;
        private Dictionary<Slice, IndexedField> _dynamicFieldsTerms;
        private FieldsCache _fieldsCache;

        private ContainerId _postingListContainerId;
        private ContainerId _storedFieldsContainerId;
        private ContainerId _entriesTermsContainerId;
        private Lookup<Int64LookupKey> _entryIdToLocation;
        private IndexFieldsMapping _dynamicFieldsMapping;
        private PostingList _largePostingListSet;
        public int? MaximumConcurrentBatchesForHnswAcceleration;

        /// <summary>
        /// Per-field set of node ids whose container record was rewritten by the just-finished
        /// vector commit. Populated as each <see cref="Hnsw.Registration.Commit"/> completes and
        /// drained by the post-commit hook via <see cref="DrainDirtyVectorSets"/>; the registration
        /// itself is cleared on field reset, so this dictionary is the only surviving handle to
        /// the dirty set after <see cref="ResetWriter"/> runs.
        /// </summary>
        private Dictionary<Slice, HashSet<long>> _dirtyVectorSets;

        /// <summary>
        /// Per-field set of node ids that the just-finished commit rewrote in the underlying
        /// HNSW container. Read by <c>CoraxIndexPersistence.RecreateSearcher</c> to apply
        /// incremental updates to the long-lived <c>HnswIndexCache</c>; null when no vector
        /// field saw any modification.
        /// </summary>
        public Dictionary<Slice, HashSet<long>> DirtyVectorSets => _dirtyVectorSets;

        private long _compactTreeDictionaryId = Constants.IndexSearcher.InvalidId;
        private EntriesToTermsTracker _entriesToTermsTracker;

        //Number of entries persisted on the disk on index writer initialization.
        private long _initialNumberOfEntries;

        private readonly IndexOperationsDumper _indexDebugDumper;

        // Encoder for the posting list.
        private FastPForEncoder _pForEncoder;

        // The last entry id (with the highest ID) that was added to the index.
        private long _lastEntryId;

        private ContextBoundNativeList<long> _tempListBuffer;

        // This is used for keeping track terms per document. The value is an entry ID of an indexed document, where the index is a reference to the actual list of terms.
        // _termsPerEntryIds and _termsPerEntryId are concatenated by index.
        // Example:
        // _termsPerEntryIds: [(Index: 0, Value: 1 (docId))]
        // _termsPerEntryId: [0: [(id(): "Doc1"), (Field1: "Term1")], ...]
        private NativeList<DocumentEntryId> _termsPerEntryIds;
        private NativeList<NativeList<RecordedTerm>> _termsPerEntryId;

        // Private context used by the index writer to store temporary data during an indexing process. We do not want to grow transaction's allocator too much for temporary data.
        private ByteStringContext _entriesAllocator;

        // Tree that contains mapping Field -> LookupTree
        private Tree _fieldsTree;

        // Used to keep track of ids of documents that have null value under certain field.
        // Mapping: [Field] -> [List of ids that have null under a field]
        private Tree _nullEntriesPostingListsTree;

        // Used to keep track of ids of documents that have no-value under certain field.
        // Mapping: [Field] -> [List of ids that have no-value under a field]
        private Tree _nonExistingEntriesPostingListsTree;

        public long GetNumberOfEntries() => _initialNumberOfEntries + _numberOfModifications;

        private int[] _suggestionsTermsLengths;

        /// <summary>
        /// Container of deleted entries' IDs. Even if we're not bulk-removing them in the Commit phase,
        /// we need to keep track of how many documents we actually deleted
        /// (e.g., when we perform a delete operation not by 'primary key,'
        /// we might end up in a situation where we try to remove the same document twice, and we need to detect it).
        ///
        /// Note: This stores document entry IDs (index-level identifiers), not ContainerEntryId (storage-level identifiers).
        /// While both are represented as long, they are semantically different concepts and should not be confused.
        /// </summary>
        private readonly HashSet<long> _deletedEntries = new();

        /// <summary>
        /// A collection used to keep track of IDs currently marked for deletion (single-call execution).
        /// It is being cleared between Delete calls.
        /// </summary>
        private ContextBoundNativeList<long> _entriesToDelete;

        private HashSet<long> _nullTermsMarkers;
        private HashSet<long> _nonExistingTermsMarkers;
        private Dictionary<long, IndexedField> _fieldsByRootPage;

        /// <summary>
        /// Context used by analyzers during indexing.
        /// </summary>
        private readonly AnalyzersContext _analyzersContext;

        internal EntryIdPaginationSupportStatus PaginationBasedOnEntryIdSupportStatus { get; private set; }


        private FieldBuffers<Slice, CompactTree.CompactKeyLookup> _textualFieldBuffers;
        private FieldBuffers<long, Int64LookupKey> _longFieldBuffers;
        private FieldBuffers<double, DoubleLookupKey> _doubleFieldBuffers;
        private FastPForDecoder _pforDecoder;

        /// <summary>
        /// Method to update dynamic mapping in runtime. 
        /// </summary>
        /// <param name="current">Updated mapping from IndexWriter object owner.</param>
        public void UpdateDynamicFieldsMapping(IndexFieldsMapping current)
        {
            _dynamicFieldsMapping = current;

            if (_dynamicFieldsTerms == null)
                return;

            foreach (var binding in _dynamicFieldsMapping)
            {
                if (_dynamicFieldsTerms.TryGetValue(binding.FieldName, out var indexedField) == false || indexedField.IsCreatedByDelete == false)
                    continue;

                //Update the indexed field in case when empty config was created by Delete operation
                var newIndexedField = new IndexedField(indexedField, binding);
                _dynamicFieldsTerms[binding.FieldName] = newIndexedField;
            }
        }

        // One of the reasons why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index writer with opening semantics, and also every new
        // writer becomes essentially a unit of work which makes reusing assets tracking more explicit.

        private IndexWriter(IndexFieldsMapping fieldsMapping, SupportedFeatures supportedFeatures)
        {
            _indexDebugDumper = new IndexOperationsDumper(fieldsMapping);
            _entryBuilder = new IndexEntryBuilder(this);
            _fieldsMapping = fieldsMapping;
            _supportedFeatures = supportedFeatures; // if not explicitly set - all features are available
            _dynamicFieldsTerms = new Dictionary<Slice, IndexedField>(SliceComparer.Instance); // avoids NRE in cases where the index does not contain a dynamic field
            _analyzersContext = new AnalyzersContext(fieldsMapping.MaximumOutputSize);

            var bufferSize = fieldsMapping!.Count;
            _knownFieldsTerms = new IndexedField[bufferSize];
            for (int i = 0; i < bufferSize; ++i)
            {
                _knownFieldsTerms[i] = new IndexedField(fieldsMapping.GetByFieldId(i), _supportedFeatures);
            }
        }

        public IndexWriter([NotNull] StorageEnvironment environment, IndexFieldsMapping fieldsMapping, SupportedFeatures supportedFeatures) : this(fieldsMapping,
            supportedFeatures)
        {
            TransactionPersistentContext transactionPersistentContext = new(true);
            _transaction = environment.WriteTransaction(transactionPersistentContext);

            _ownsTransaction = true;
            Init();
        }

        public IndexWriter([NotNull] Transaction tx, IndexFieldsMapping fieldsMapping, SupportedFeatures supportedFeatures) : this(fieldsMapping, supportedFeatures)
        {
            _transaction = tx;

            _ownsTransaction = false;
            Init();
        }

        private void Init()
        {
            Debug.Assert(_transaction.LowLevelTransaction.Flags == TransactionFlags.ReadWrite);
            _compactKeyScope = new(_transaction.LowLevelTransaction);
            _postingListContainerId = _transaction.OpenContainer(Constants.IndexWriter.PostingListsSlice);
            _storedFieldsContainerId = _transaction.OpenContainer(Constants.IndexWriter.StoredFieldsSlice);
            _entriesTermsContainerId = _transaction.OpenContainer(Constants.IndexWriter.EntriesTermsContainerSlice);
            _entryIdToLocation = _transaction.LookupFor<Int64LookupKey>(Constants.IndexWriter.EntryIdToLocationSlice);
            _jsonOperationContext = JsonOperationContext.ShortTermSingleUse();
            _fieldsTree = _transaction.CreateTree(Constants.IndexWriter.FieldsSlice);

            _indexMetadata = _transaction.CreateTree(Constants.IndexMetadataSlice);
            Debug.Assert(_indexMetadata is not null);

            _initialNumberOfEntries = _indexMetadata.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0;
            var paginationBasedOnEntryIdSupportStatus = _indexMetadata.ReadInt64(Constants.IndexWriter.PaginationBasedOnEntryIdSupportStatus);
            if (paginationBasedOnEntryIdSupportStatus.HasValue == false)
            {
                if (_supportedFeatures.PaginationBasedOnEntryId)
                {
                    _indexMetadata.Add(Constants.IndexWriter.PaginationBasedOnEntryIdSupportStatus, (long)EntryIdPaginationSupportStatus.Supported);
                    PaginationBasedOnEntryIdSupportStatus = EntryIdPaginationSupportStatus.Supported;
                }
                else
                {
                    PaginationBasedOnEntryIdSupportStatus = EntryIdPaginationSupportStatus.Disabled;
                }
            }
            else
            {
                PaginationBasedOnEntryIdSupportStatus = (EntryIdPaginationSupportStatus)paginationBasedOnEntryIdSupportStatus.Value;
            }

            _lastEntryId = _indexMetadata?.ReadInt64(Constants.IndexWriter.LastEntryIdSlice) ?? 0;
            _documentBoost = _transaction.FixedTreeFor(Constants.DocumentBoostSlice, sizeof(float));
            _nullEntriesPostingListsTree = _transaction.CreateTree(Constants.IndexWriter.NullPostingLists);
            _nonExistingEntriesPostingListsTree = _transaction.CreateTree(Constants.IndexWriter.NonExistingPostingLists);
            _entriesAllocator = new ByteStringContext(SharedMultipleUseFlag.None);

            _tempListBuffer = new ContextBoundNativeList<long>(_entriesAllocator);
            _smallPostingListWorkingBuffer = new ContextBoundNativeList<long>(_entriesAllocator);

            _entriesToTermsTracker = new(this);

            _pforDecoder = new FastPForDecoder(_entriesAllocator);
            ReadPersistedVectorRootPages(out _persistedVectorRootPages);

            // We want to use the LLT allocator because the list will be small most of the time,
            // and we do not want to manually handle updating the allocator after a flush.
            _entriesToDelete = new(_transaction.LowLevelTransaction.Allocator);
            _entriesToTermsTree = _transaction.CreateTree(Constants.IndexWriter.EntriesToTermsSlice);
        }

        private void InitializeFieldRootPage(IndexedField field)
        {
            if (field.FieldRootPage == Constants.IndexWriter.UninitializedFieldRootPage)
            {
                _fieldsTree ??= _transaction.CreateTree(Constants.IndexWriter.FieldsSlice);
                field.FieldRootPage = _fieldsCache.GetFieldRootPage(field.Name, _fieldsTree);
            }
        }

        private void InitializeFieldRootPageForTermsVector(IndexedField field)
        {
            Debug.Assert(field.FieldIndexingMode is FieldIndexingMode.Search, "field.FieldIndexingMode is FieldIndexingMode.Search");
            Debug.Assert(_supportedFeatures.PhraseQuery, "_phraseQuerySupport");

            if (field.TermsVectorFieldRootPage == Constants.IndexWriter.InvalidPageId)
            {
                _fieldsTree ??= _transaction.CreateTree(Constants.IndexWriter.FieldsSlice);
                _transaction.Allocator.Allocate(field.Name.Size + Constants.PhraseQuerySuffix.Length, out var memory);
                var memAsSpan = memory.ToSpan();
                field.Name.AsReadOnlySpan().CopyTo(memAsSpan);
                Constants.PhraseQuerySuffix.CopyTo(memAsSpan.Slice(field.Name.Size));
                var storedName = new Slice(memory);
                field.TermsVectorFieldRootPage = _fieldsCache.GetFieldRootPage(storedName, _fieldsTree);
            }
        }

        public IndexEntryBuilder Update(ReadOnlySpan<byte> key)
        {
            // We do not dispose because we will be storing the slice in the hash set.
            Slice.From(_transaction.Allocator, key, ByteStringType.Immutable, out var keySlice);

            if (TryDeleteEntry(keySlice, out var entryId))
            {
                _numberOfModifications++;
            }
            else
            {
                entryId = InitBuilder();
            }



            _indexedEntries.Add(keySlice); // Register entry by key.
            int index = InsertTermsPerEntry(entryId);
            _entryBuilder.Init(entryId, index, keySlice);
            return _entryBuilder;
        }

        private int InsertTermsPerEntry(DocumentEntryId entryId)
        {
            int index = _termsPerEntryId.Count;
            _termsPerEntryId.EnsureCapacityFor(_entriesAllocator, 1);
            _termsPerEntryIds.EnsureCapacityFor(_entriesAllocator, 1);
            _termsPerEntryId.AddByRefUnsafe() = new NativeList<RecordedTerm>();
            _termsPerEntryIds.AddUnsafe(entryId);
            return index;
        }

        public IndexEntryBuilder Index(string key) => Index(Encoding.UTF8.GetBytes(key));

        public IndexEntryBuilder Index(ReadOnlySpan<byte> key)
        {
            DocumentEntryId entryId = InitBuilder();

            // We do not dispose because we will be storing the slice in the hash set.
            Slice.From(_transaction.Allocator, key, ByteStringType.Immutable, out var keySlice);
            var isUnique = _indexedEntries.Add(keySlice); // Register entry by key.
            if (isUnique == false && PaginationBasedOnEntryIdSupportStatus == EntryIdPaginationSupportStatus.Supported)
                DisablePaginationBasedOnEntryIdSupport();

            int index = InsertTermsPerEntry(entryId);
            _entryBuilder.Init(entryId, index, keySlice);
            return _entryBuilder;
        }

        private void DisablePaginationBasedOnEntryIdSupport()
        {
            PaginationBasedOnEntryIdSupportStatus = EntryIdPaginationSupportStatus.Disabled;
            _indexMetadata.Add(Constants.IndexWriter.PaginationBasedOnEntryIdSupportStatus, (long)EntryIdPaginationSupportStatus.Disabled);
        }

        private DocumentEntryId InitBuilder()
        {
            if (_entryBuilder.Active)
                ThrowPreviousBuilderIsNotDisposed();

            _numberOfModifications++;
            var entryId = ++_lastEntryId;

            return new DocumentEntryId(entryId);
        }

        /// <summary>
        /// Document Boost should add priority to some documents but also should not be the main component of boosting.
        /// The natural logarithm slows down our scoring increase for a document so that the ranking calculated at query time is not forgotten.
        /// We've to add entry container id (without frequency etc) here because in 'SortingMatch' we have already decoded ids.
        /// </summary>
        /// <param name="entryId">Document id</param>
        /// <param name="documentBoost">Document boost value</param>
        private void BoostEntry(DocumentEntryId entryId, float documentBoost)
        {
            if (documentBoost.AlmostEquals(1f))
            {
                // We don't store `1` but if user updates boost value to 1
                // we've to delete the previous one, we don't need to do this explicitly
                // since we'll delete it during ProcessDeletes()
                return;
            }

            // probably user wants this to be at the same end.
            if (documentBoost <= 0f)
                documentBoost = 0;

            documentBoost = MathF.Log(documentBoost + 1); // ensure we've a positive number
            _boostedDocs ??= new();
            _boostedDocs.Add((entryId, documentBoost));
        }


        /// <summary>Remove a document boosting from the tree.</summary>
        /// <param name="entryId">Document entry id</param>
        private void RemoveDocumentBoost(DocumentEntryId entryId)
        {
            _documentBoost.Delete((long)entryId);
        }

        private IndexedField GetDynamicIndexedField(ByteStringContext context, string currentFieldName)
        {
            using var _ = Slice.From(context, currentFieldName, out var slice);
            return GetDynamicIndexedField(slice);
        }

        private IndexedField GetDynamicIndexedField(ByteStringContext context, Span<byte> currentFieldName, bool createdByDelete = false)
        {
            using var _ = Slice.From(context, currentFieldName, out var slice);
            return GetDynamicIndexedField(slice, createdByDelete);
        }


        private IndexedField GetDynamicIndexedField(Slice fieldName, bool createdByDelete = false)
        {
            //We have to use transaction context here for storing slices in _dynamicFieldsTerms since we may reset other
            //allocators during the document insertion.
            var context = _transaction.LowLevelTransaction.Allocator;
            _dynamicFieldsTerms ??= new(SliceComparer.Instance);
            if (_dynamicFieldsTerms.TryGetValue(fieldName, out var indexedField))
            {
                return indexedField;
            }

            IndexedField source = null;
            if (_fieldsMapping.TryGetByFieldName(fieldName, out var knownField))
                source = _knownFieldsTerms[knownField.FieldId];

            var clonedFieldName = fieldName.Clone(context);
            if (_dynamicFieldsMapping?.TryGetByFieldName(clonedFieldName, out var binding) is true)
            {
                indexedField = source?.CreateVirtualIndexedField(binding, createdByDelete)
                               ?? new IndexedField(Constants.IndexWriter.DynamicField, binding.FieldName, binding.FieldNameLong,
                                   binding.FieldNameDouble, binding.FieldTermTotalSumField, binding.Analyzer,
                                   binding.FieldIndexingMode, binding.HasSuggestions, binding.ShouldStore, _supportedFeatures, binding.VectorOptions);
            }
            else
            {
                indexedField = CreateDynamicField(null, FieldIndexingMode.Normal);
            }

            _dynamicFieldsTerms[clonedFieldName] = indexedField;
            InitializeFieldRootPage(indexedField);
            return indexedField;

            IndexedField CreateDynamicField(Analyzer analyzer, FieldIndexingMode mode)
            {
                IndexFieldsMappingBuilder.GetFieldNameForLongs(context, clonedFieldName, out var fieldNameLong);
                IndexFieldsMappingBuilder.GetFieldNameForDoubles(context, clonedFieldName, out var fieldNameDouble);
                IndexFieldsMappingBuilder.GetFieldForTotalSum(context, clonedFieldName, out var nameSum);
                var field = source is null
                    ? new IndexedField(Constants.IndexWriter.DynamicField, clonedFieldName, fieldNameLong, fieldNameDouble, nameSum, analyzer, mode,
                        hasSuggestions: false, shouldStore: false, _supportedFeatures, vectorOptions: null, isCreatedByDelete: createdByDelete)
                    : source.CreateVirtualIndexedField(
                        new IndexFieldBinding(Constants.IndexWriter.DynamicField, clonedFieldName, fieldNameLong, fieldNameDouble, nameSum, true, analyzer,
                            hasSuggestions: false, FieldIndexingMode.Normal), createdByDelete);
                return field;
            }
        }

        private void AddSuggestions(IndexedField field, Slice slice)
        {
            _hasSuggestions = true;
            field.Suggestions ??= new Dictionary<Slice, int>(SliceComparer.Instance);

            if (_suggestionsTermsLengths == null || _suggestionsTermsLengths.Length < slice.Size)
                _suggestionsTermsLengths = new int[Math.Max(2 * slice.Size, 32)];

            var termsLength = _suggestionsTermsLengths;

            var keys = SuggestionsKeys.Generate(_entriesAllocator, Constants.Suggestions.DefaultNGramSize, slice.AsReadOnlySpan(), termsLength, out int keysCount);

            var suggestionsToAdd = field.Suggestions;

            int idx = 0;
            int currentOffset = 0;
            while (idx < keysCount)
            {
                int keySize = termsLength[idx];

                var key = new Slice(_entriesAllocator.Slice(keys, currentOffset, keySize, ByteStringType.Immutable));
                if (suggestionsToAdd.TryGetValue(key, out int counter) == false)
                    counter = 0;

                counter++;
                suggestionsToAdd[key] = counter;

                currentOffset += keySize;
                idx++;
            }
        }

        private void RemoveSuggestions(IndexedField field, ReadOnlySpan<byte> sequence)
        {
            _hasSuggestions = true;
            field.Suggestions ??= new Dictionary<Slice, int>();

            if (_suggestionsTermsLengths == null || _suggestionsTermsLengths.Length < sequence.Length)
                _suggestionsTermsLengths = new int[Math.Max(2 * sequence.Length, 32)];

            var termsLength = _suggestionsTermsLengths;

            var keys = SuggestionsKeys.Generate(_entriesAllocator, Constants.Suggestions.DefaultNGramSize, sequence, termsLength, out int keysCount);

            var suggestionsToRemove = field.Suggestions;

            int idx = 0;
            int currentOffset = 0;
            while (idx < keysCount)
            {
                int keySize = termsLength[idx];

                var key = new Slice(_entriesAllocator.Slice(keys, currentOffset, keySize, ByteStringType.Immutable));
                if (suggestionsToRemove.TryGetValue(key, out int counter) == false)
                    counter = 0;

                counter--;
                suggestionsToRemove[key] = counter;
                idx++;

                currentOffset += keySize;
            }
        }

        internal static ByteStringContext<ByteStringMemoryCache>.InternalScope CreateNormalizedTerm(ByteStringContext context, ReadOnlySpan<byte> value, out Slice slice)
        {
            if (value.Length <= Constants.Terms.MaxLength)
                return Slice.From(context, value, ByteStringType.Mutable, out slice);

            return UnlikelyCreateLargeTerm(context, value, out slice);
        }

        private static ByteStringContext<ByteStringMemoryCache>.InternalScope UnlikelyCreateLargeTerm(ByteStringContext context, ReadOnlySpan<byte> value,
            out Slice slice)
        {
            int hashStartingPoint = Constants.Terms.MaxLength - 2 * sizeof(ulong);
            ulong hash = Hashing.XXHash64.Calculate(value.Slice(hashStartingPoint));

            Span<byte> localValue = stackalloc byte[Constants.Terms.MaxLength];
            value.Slice(0, Constants.Terms.MaxLength).CopyTo(localValue);
            int hexSize = Numbers.FillAsHex(localValue.Slice(hashStartingPoint), hash);
            Debug.Assert(Constants.Terms.MaxLength == hashStartingPoint + hexSize, "Constants.Terms.MaxLength == hashStartingPoint + hexSize");

            return Slice.From(context, localValue, ByteStringType.Mutable, out slice);
        }

        /// <summary>
        /// Handle removals found in RecordAndPrepareDocumentsIdsForDeletion.
        /// </summary>
        private void ProcessCurrentDeletes()
        {
            if (_nullTermsMarkers is null || _nonExistingTermsMarkers is null)
            {
                Querying.IndexSearcher.LoadSpecialTermMarkers(_nullEntriesPostingListsTree, out _nullTermsMarkers);
                Querying.IndexSearcher.LoadSpecialTermMarkers(_nonExistingEntriesPostingListsTree, out _nonExistingTermsMarkers);
            }

            if (_compactTreeDictionaryId == Constants.IndexSearcher.InvalidId)
                _compactTreeDictionaryId = CompactTree.GetDictionaryId(_transaction.LowLevelTransaction);

            _fieldsByRootPage ??= GetIndexedFieldByRootPage(_fieldsTree);

            foreach (var entryToDelete in _entriesToDelete)
            {
                _termsPerEntryId.EnsureCapacityFor(_entriesAllocator, 1);
                if (_entryIdToLocation.TryRemove(entryToDelete, out var entryTermsIdLong) == false)
                    ThrowUnableToLocateEntry(entryToDelete);

                var documentToDelete = new DocumentEntryId(entryToDelete);
                ContainerEntryId entryTermsId = (ContainerEntryId)entryTermsIdLong;
                RemoveDocumentBoost(documentToDelete);
                Container.Get(_transaction.LowLevelTransaction, entryTermsId, out var entryTerms);
                var termsPerEntryIndex = InsertTermsPerEntry(documentToDelete);
                RecordTermDeletionsForEntry(entryTerms, _transaction.LowLevelTransaction, _fieldsByRootPage, _nullTermsMarkers, _nonExistingTermsMarkers,
                    _compactTreeDictionaryId, documentToDelete, termsPerEntryIndex);


                Container.Delete(_transaction.LowLevelTransaction, _entriesTermsContainerId, entryTermsId);
            }

            _entriesToDelete.Clear();
        }

        private void RecordTermDeletionsForEntry(Container.Item entryTerms, LowLevelTransaction llt, Dictionary<long, IndexedField> fieldsByRootPage,
            HashSet<long> nullTermMarkers, HashSet<long> nonExistingTermMarkers, long dicId, DocumentEntryId entryToDelete, int termsPerEntryIndex)
        {
            var reader = new EntryTermsReader(llt, nullTermMarkers, nonExistingTermMarkers, entryTerms.Address, entryTerms.Length, dicId, _persistedVectorRootPages);

            reader.Reset();
            while (reader.MoveNextStoredField())
            {
                // Null/empty is not stored in a container, just exists as a marker.
                if (reader.TermId == Constants.IndexSearcher.InvalidId)
                    continue;

                if (reader.IsVectorHash)
                {
                    bool exists = fieldsByRootPage.TryGetValue(reader.FieldRootPage, out var field);
                    PortableExceptions.ThrowIfNot<InvalidOperationException>(exists, "Tried to remove vector but couldn't find the associated indexed field.");
                    var vectorIndexer = field!.GetVectorIndexer(_transaction.LowLevelTransaction);
                    Debug.Assert(vectorIndexer != null && reader.StoredField is { Length: 32 });
                    vectorIndexer.Remove((long)entryToDelete, reader.StoredField.Value.ToSpan());
                }

                Container.Delete(llt, _storedFieldsContainerId, new ContainerEntryId(reader.TermId));
            }

            reader.Reset();
            while (reader.MoveNext())
            {
                if (fieldsByRootPage.TryGetValue(reader.FieldRootPage, out var field) == false)
                {
                    ThrowUnableToFindMatchingField(reader);
                }

                if (reader.IsNull)
                {
                    RemoveMarkerTerm(field, reader, Constants.NullValueSlice, entryToDelete, termsPerEntryIndex);
                    continue;
                }

                if (reader.IsNonExisting)
                {
                    RemoveMarkerTerm(field, reader, Constants.NonExistingValueSlice, entryToDelete, termsPerEntryIndex);
                    continue;
                }

                if (reader.IsVectorHash)
                    PortableExceptions.Throw<InvalidOperationException>($"Field with vector object should not have any textual/numerical/etc values!");

                var decodedKey = reader.Current.Decoded();
                var scope = Slice.From(_entriesAllocator, decodedKey, out Slice termSlice);
                if (field.HasSuggestions)
                    RemoveSuggestions(field, decodedKey);

                // RavenDB-25907: Sentinel value pattern for atomic Dictionary+Storage update.
                ref var termLocation = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Textual, termSlice, out var exists);
                if (exists == false || termLocation == Constants.IndexWriter.InvalidStorageIndex)
                {
                    termLocation = Constants.IndexWriter.InvalidStorageIndex;
                    var newIndex = field.Storage.Count;
                    field.Storage.AddByRef(new EntriesModifications(decodedKey.Length));
                    termLocation = newIndex;

                    scope = default; // We don't want to reclaim the term name
                }

                ref var term = ref field.Storage.GetAsRef(termLocation);
                term.Removal(_entriesAllocator, entryToDelete, termsPerEntryIndex, reader.Frequency, InserterMode.ExactInsert);
                scope.Dispose();

                if (reader.HasNumeric == false)
                    continue;

                // RavenDB-25907: Sentinel value pattern for atomic Dictionary+Storage update.
                ref var longTermLocation = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Longs, reader.CurrentLong, out exists);
                if (exists == false || longTermLocation == Constants.IndexWriter.InvalidStorageIndex)
                {
                    longTermLocation = Constants.IndexWriter.InvalidStorageIndex;
                    var newIndex = field.Storage.Count;
                    field.Storage.AddByRef(new EntriesModifications(sizeof(long)));
                    longTermLocation = newIndex;
                }

                term = ref field.Storage.GetAsRef(longTermLocation);
                term.Removal(_entriesAllocator, entryToDelete, termsPerEntryIndex, freq: 1, InserterMode.Numerical);

                // RavenDB-25907: Sentinel value pattern for atomic Dictionary+Storage update.
                ref var doubleTermLocation = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Doubles, reader.CurrentDouble, out exists);
                if (exists == false || doubleTermLocation == Constants.IndexWriter.InvalidStorageIndex)
                {
                    doubleTermLocation = Constants.IndexWriter.InvalidStorageIndex;
                    var newIndex = field.Storage.Count;
                    field.Storage.AddByRef(new EntriesModifications(sizeof(double)));
                    doubleTermLocation = newIndex;
                }

                term = ref field.Storage.GetAsRef(doubleTermLocation);
                term.Removal(_entriesAllocator, entryToDelete, termsPerEntryIndex, freq: 1, InserterMode.Numerical);
            }
        }

        private void RemoveMarkerTerm(IndexedField field, EntryTermsReader reader, Slice termSlice, DocumentEntryId entryToDelete, int termsPerEntryIndex)
        {
            // RavenDB-25907: Sentinel value pattern for atomic Dictionary+Storage update.
            ref var termLocation = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Textual, termSlice, out var exists);
            if (exists == false || termLocation == Constants.IndexWriter.InvalidStorageIndex)
            {
                termLocation = Constants.IndexWriter.InvalidStorageIndex;
                var newIndex = field.Storage.Count;
                field.Storage.AddByRef(new EntriesModifications(1));
                termLocation = newIndex;
                // We dont want to reclaim the term name
            }

            ref var term = ref field.Storage.GetAsRef(termLocation);
            term.Removal(_entriesAllocator, entryToDelete, termsPerEntryIndex, reader.Frequency, InserterMode.ExactInsert);
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

        private void ReadPersistedVectorRootPages(out long[] persistedVectorRootPages)
        {
            if (_indexMetadata != null && _indexMetadata.TryRead(Constants.IndexWriter.VectorFieldsRootPagesSlice, out var reader))
            {
                persistedVectorRootPages = reader.ToUnmanagedSpan<long>().ToSpan().ToArray();
            }
            else
            {
                persistedVectorRootPages = [];
            }
        }

        private void PersistVectorRootPages()
        {
            if (_newVectorRootPages is null)
                return;

            foreach (var vf in _persistedVectorRootPages)
                _newVectorRootPages.Add(vf);

            var newList = _newVectorRootPages.ToArray();
            Sort.Run(newList);

            using (_indexMetadata.DirectAdd(Constants.IndexWriter.VectorFieldsRootPagesSlice, newList.Length * sizeof(long), out var destination))
                newList.CopyTo(new Span<long>(destination, newList.Length));

            _newVectorRootPages = null;
            _persistedVectorRootPages = null;
        }

        private void RegisterVectorRootPage(long rootPage)
        {
            if (_persistedVectorRootPages.AsSpan().Contains(rootPage))
                return;

            _newVectorRootPages ??= new();
            _newVectorRootPages.Add(rootPage);
        }

        private Dictionary<long, IndexedField> GetIndexedFieldByRootPage(Tree fieldsTree, bool isFromDelete = true)
        {
            var pageToField = new Dictionary<long, IndexedField>();

            var it = fieldsTree.Iterate(prefetch: false);
            if (it.Seek(Slices.BeforeAllKeys))
            {
                do
                {
                    var state = (LookupState*)it.CreateReaderForCurrent().Base;
                    if (state->RootObjectType != RootObjectType.Lookup)
                        continue;

                    var found = _fieldsMapping.TryGetByFieldName(it.CurrentKey, out var field);
                    if (found == false)
                    {
                        if (it.CurrentKey.EndsWith(Constants.IndexWriter.DoubleTreeSuffix) || it.CurrentKey.EndsWith(Constants.IndexWriter.LongTreeSuffix))
                            continue; // numeric postfix values
                        var dynamicIndexedField = GetDynamicIndexedField(_entriesAllocator, it.CurrentKey.AsSpan(), createdByDelete: isFromDelete);
                        pageToField.Add(state->RootPage, dynamicIndexedField);
                    }
                    else
                    {
                        pageToField.Add(state->RootPage, _knownFieldsTerms[field.FieldId]);
                    }
                } while (it.MoveNext());
            }

            return pageToField;
        }

        public bool TryDeleteEntry(string term)
        {
            using var _ = Slice.From(_transaction.Allocator, term, ByteStringType.Immutable, out var termSlice);
            return TryDeleteEntry(termSlice, out var _);
        }

        public bool TryDeleteEntry(ReadOnlySpan<byte> term)
        {
            using var __ = Slice.From(_transaction.Allocator, term, ByteStringType.Immutable, out var termSlice);
            return TryDeleteEntry(termSlice, out _);
        }

        private bool TryDeleteEntry(Slice termSlice, out DocumentEntryId entryId)
        {
            if (_indexedEntries.Contains(termSlice) == false)
            {
                _compactKeyScope.Key.Set(termSlice);
                var exists = _fieldsTree.CompactTreeFor(_fieldsMapping.GetByFieldId(Constants.IndexWriter.PrimaryKeyFieldId).FieldName)
                    .TryGetValue(_compactKeyScope.Key, out var containerId);
                if (exists)
                {
                    // note that the containerId may be a single value or many(!), if it is many items
                    // we'll delete them, but treat this as a _new_ entry, not an update to an existing
                    // one
                    RecordAndPrepareDocumentsIdsForDeletion(containerId, out var setsAreDisjoint, out var isSingleDocument, out var singleDocumentEntryId);
                    entryId = isSingleDocument ? singleDocumentEntryId : DocumentEntryId.Invalid;

                    Debug.Assert(isSingleDocument || setsAreDisjoint,
                        $"A single document can be deleted twice (delete + update), however if it's not a single document, the sets are supposed to be disjoint.");


                    ProcessCurrentDeletes();
                    return isSingleDocument;
                }

                entryId = DocumentEntryId.Invalid;
                return false;
            }

            FlushBatch();
            return TryDeleteEntry(termSlice, out entryId);
        }

        public void DeleteByPrefix(ReadOnlySpan<byte> prefix)
        {
            using var __ = Slice.From(_transaction.Allocator, prefix, ByteStringType.Immutable, out var prefixSlice);
            var hasPrefixInCurrentlyIndexedEntries = _indexedEntries.Any(id => SliceComparer.StartWith(id, prefixSlice));
            var requiresFlushingBatch = hasPrefixInCurrentlyIndexedEntries;

            if (hasPrefixInCurrentlyIndexedEntries == false)
            {
                var primaryKeyTree = _fieldsTree.CompactTreeFor(_fieldsMapping.GetByFieldId(Constants.IndexWriter.PrimaryKeyFieldId).FieldName);
                var treeIterator = primaryKeyTree.Iterate();
                treeIterator.Seek(prefixSlice);

                while (requiresFlushingBatch == false && treeIterator.MoveNext(out var currentKey, out var postingListId, out _))
                {
                    // Since we're seeking, we will have a key that has exactly the same prefix or prefix + 1 (in terms of order).
                    if (currentKey.Decoded().StartsWith(prefixSlice) == false)
                        break;

                    RecordAndPrepareDocumentsIdsForDeletion(postingListId, out bool setsAreDisjoint, out _, out _);
                    ProcessCurrentDeletes();
                    requiresFlushingBatch = setsAreDisjoint == false;
                }
            }

            if (requiresFlushingBatch)
            {
                FlushBatch();
                DeleteByPrefix(prefix);
            }
        }

        private void FlushBatch()
        {
            // We cannot handle modifications to the same entry in the same batch, so we cheat
            // we do a side channel flush at this point, then reset the state of the writer back to its initial level
            bool prevValue = _ownsTransaction;
            _ownsTransaction = false;
            try
            {
                Commit();
                ResetWriter();
            }
            finally
            {
                _ownsTransaction = prevValue;
            }
        }

        private void ResetWriter()
        {
            _indexedEntries.Clear();
            _deletedEntries.Clear();
            _entriesToTermsTracker.ClearEntriesForTerm();

            // We have to reset markers and root pages because we may have created new ones in the commit phase.
            _nullTermsMarkers = null;
            _nonExistingTermsMarkers = null;
            _fieldsByRootPage = null;
            foreach (var term in _knownFieldsTerms)
            {
                term.Clear();
            }

            if (_dynamicFieldsTerms != null)
            {
                foreach (var (_, field) in _dynamicFieldsTerms)
                {
                    field.Clear();
                }
            }

            // PERF: Since we are resetting the entries allocator, we can avoid disposing every internal data structure
            // that uses the allocator internally. 
            _entriesAllocator.Reset();
            _entriesToTermsTracker.Reset();
            _smallPostingListWorkingBuffer = new(_entriesAllocator);

            _tempListBuffer = new(_entriesAllocator);
            _termsPerEntryId = new NativeList<NativeList<RecordedTerm>>();
            _termsPerEntryIds = new NativeList<DocumentEntryId>();
            _numberOfModifications = 0;
            _numberOfTermModifications = 0;
            _initialNumberOfEntries = _indexMetadata?.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0;
            ReadPersistedVectorRootPages(out _persistedVectorRootPages);

            _pforDecoder = new FastPForDecoder(_entriesAllocator);
        }

        public void TryDeleteEntryByField(string field, string term)
        {
            if (_fieldsMapping.TryGetByFieldName(field, out var binding) && binding.FieldId == Constants.IndexWriter.PrimaryKeyFieldId)
            {
                TryDeleteEntry(term);
                return;
            }

            long idInTree;
            using (Slice.From(_entriesAllocator, term, ByteStringType.Immutable, out var termSlice))
            using (Slice.From(_entriesAllocator, field, ByteStringType.Immutable, out var fieldSlice))
            {
                if (TryGetEntryTermId(fieldSlice, termSlice.AsSpan(), out idInTree) == false)
                    return;
            }

            RecordAndPrepareDocumentsIdsForDeletion(idInTree, out var setsAreDisjoint, out _, out _);
            ProcessCurrentDeletes();

            if (setsAreDisjoint == false)
            {
                FlushBatch();
                TryDeleteEntryByField(field, term);
            }
        }

        /// <summary>
        /// Record term for deletion from Index.
        /// </summary>
        /// <param name="idInTree">With frequencies and container type.</param>
        /// <param name="setsAreDisjoint">Intersection between PostingList and _deletedEntries. We may use it as indicator for flushing batch.</param>
        [SkipLocalsInit]
        private void RecordAndPrepareDocumentsIdsForDeletion(long postingListId, out bool setsAreDisjoint, out bool isSingleDocument, out DocumentEntryId singleDocumentEntryId)
        {
            Debug.Assert(_entriesToDelete.Count == 0);

            var countOfAlreadyDeletedEntries = _deletedEntries.Count;
            setsAreDisjoint = true;

            if ((postingListId & (long)TermIdMask.EnsureIsSingleMask) == (long)TermIdMask.Single)
            {
                // Encoding duality: TermIdMask.Single stores DocumentEntryId in bits, posting lists store ContainerEntryId.
                // Use DecodeAndDiscardFrequency for Single, GetContainerId for posting-lists.
                singleDocumentEntryId = EntryIdEncodings.DecodeAndDiscardFrequency(postingListId);
                Debug.Assert(singleDocumentEntryId.IsValid);
                var isNewDocument = _deletedEntries.Add((long)singleDocumentEntryId);
                if (isNewDocument)
                    _entriesToDelete.Add((long)singleDocumentEntryId);
                setsAreDisjoint &= isNewDocument;
                _numberOfModifications -= _deletedEntries.Count - countOfAlreadyDeletedEntries;
                isSingleDocument = true;
                return;
            }

            // For posting lists, extract the container ID
            var containerEntryId = EntryIdEncodings.GetContainerId(postingListId);

            const int bufferSize = 1024;
            var bufferPtr = stackalloc long[bufferSize];
            var buffer = new Span<long>(bufferPtr, bufferSize);
            isSingleDocument = false;
            singleDocumentEntryId = DocumentEntryId.Invalid;
            if ((postingListId & (long)TermIdMask.PostingList) != 0)
            {
                var setSpace = Container.GetMutable(_transaction.LowLevelTransaction, containerEntryId);
                ref var setState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);

                using var set = new PostingList(_transaction.LowLevelTransaction, Slices.Empty, setState);
                var iterator = set.Iterate();

                while (iterator.Fill(buffer, out var read))
                    AddDocumentsToDeletion(buffer, read, ref setsAreDisjoint);
            }

            if ((postingListId & (long)TermIdMask.SmallPostingList) != 0)
            {
                Container.Get(_transaction.LowLevelTransaction, containerEntryId, out var smallSet);
                // combine with existing value
                _ = VariableSizeEncoding.Read<int>(smallSet.Address, out var pos);
                _pforDecoder.Init(smallSet.Address + pos, smallSet.Length - pos);

                while (_pforDecoder.Read(bufferPtr, bufferSize) is var read and > 0)
                    AddDocumentsToDeletion(buffer, read, ref setsAreDisjoint);
            }

            _numberOfModifications -= _deletedEntries.Count - countOfAlreadyDeletedEntries;

            void AddDocumentsToDeletion(Span<long> entries, int read, ref bool setsAreDisjoint)
            {
                // since this is also encoded, we've to delete frequency and container type as well
                EntryIdEncodings.DecodeAndDiscardFrequency(entries, read);
                foreach (var entryId in entries[..read])
                {
                    Debug.Assert(entryId > 0);
                    var isNewDocument = _deletedEntries.Add(entryId);
                    setsAreDisjoint &= isNewDocument;
                    if (isNewDocument)
                        _entriesToDelete.Add(entryId);
                }
            }
        }

        /// <summary>
        /// Get TermId (id of container) from FieldTree 
        /// </summary>
        /// <param name="idInTree">Has frequency and container type inside idInTree.</param>
        /// <returns></returns>
        private bool TryGetEntryTermId(Slice fieldName, ReadOnlySpan<byte> term, out long idInTree)
        {
            var fieldTree = _fieldsTree.CompactTreeFor(fieldName);

            // We need to normalize the term in case we have a term bigger than MaxTermLength.
            using var __ = CreateNormalizedTerm(_entriesAllocator, term, out var termSlice);

            var termValue = termSlice.AsReadOnlySpan();
            return fieldTree.TryGetValue(termValue, out idInTree);
        }

        public void Commit(CancellationToken token = default) => Commit<EmptyStatsScope>(default, token);

        public void Commit<TStatsScope>(TStatsScope stats, CancellationToken token)
            where TStatsScope : struct, ICoraxStatsScope
        {
            _indexDebugDumper.Commit();
            using var _ = _entriesAllocator.Allocate(Container.MaxSizeInsideContainerPage, out Span<byte> workingBuffer);


            Tree entriesToSpatialTree = _transaction.CreateTree(Constants.IndexWriter.EntriesToSpatialSlice);
            _indexMetadata.Increment(Constants.IndexWriter.NumberOfEntriesSlice, _numberOfModifications);
            _indexMetadata.Increment(Constants.IndexWriter.NumberOfTermsInIndex, _numberOfTermModifications);
            _indexMetadata.Add(Constants.IndexWriter.LastEntryIdSlice, _lastEntryId);
            _pForEncoder = new FastPForEncoder(_entriesAllocator);

            if (_boostedDocs != null)
                AppendDocumentsBoost();

            // Instead of going through fields by their IDs number, let's go by the amount of textual fields in ascending order.
            // In the case of static indexes, all entries should have (except for some dynamic fields) pretty much exactly the same number of fields inside.
            // So, if a field has fewer textual values (which is a good point because ALL fields have to have this value) than another, that means the PostingList inside it is bigger (except for situations with dynamic fields).
            // We're hoping to start with the biggest posting lists possible at the very beginning to allocate and release huge chunks of memory and reuse them for fields with smaller posting lists.
            var fieldCount = _knownFieldsTerms.Length + (_dynamicFieldsTerms?.Count ?? 0);
            var sortedFieldsBuffer = ArrayPool<IndexedField>.Shared.Rent(fieldCount);
            Span<int> uniquePostingList = _knownFieldsTerms.Length > 256 ? new int[fieldCount] : stackalloc int[fieldCount];
            var fieldIt = 0;
            foreach (var field in _knownFieldsTerms.AsSpan())
                (sortedFieldsBuffer[fieldIt], uniquePostingList[fieldIt++]) = (field, field.GetApproximateNumberOfTerms());
            if (_dynamicFieldsTerms != null)
            {
                foreach (var field in _dynamicFieldsTerms.Values)
                    (sortedFieldsBuffer[fieldIt], uniquePostingList[fieldIt++]) = (field, field.GetApproximateNumberOfTerms());
            }

            var sortedFields = sortedFieldsBuffer.AsSpan(0, fieldIt);
            uniquePostingList.Sort(sortedFields);
            foreach (var indexedField in sortedFields)
            {
                token.ThrowIfCancellationRequested();
                stats.SetAllocatedUnmanagedBytes(_entriesAllocator?._totalAllocated ?? 0);

                //Dynamic terms will be indexed with explicit field terms.
                if (indexedField.IsVirtual)
                {
                    continue;
                }

                using var staticFieldScope = stats.For(indexedField.NameForStatistics);

                if (indexedField.VectorIndexer != null)
                {
                    using var __ = staticFieldScope.For(CommitOperation.VectorValues);
                    RegisterVectorRootPage(indexedField.FieldRootPage);
                    if (MaximumConcurrentBatchesForHnswAcceleration != null)
                        indexedField.VectorIndexer.MaxConcurrentBatches = MaximumConcurrentBatchesForHnswAcceleration.Value;
                    indexedField.VectorIndexer.Commit(token);

                    // Snapshot DirtyNodeIds before ResetWriter clears the field's VectorIndexer.
                    // The post-commit hook (CoraxIndexPersistence.RecreateSearcher) consumes this
                    // to apply incremental updates against the long-lived HnswIndexCache.
                    if (indexedField.VectorIndexer.DirtyNodeIds.Count > 0)
                    {
                        _dirtyVectorSets ??= new Dictionary<Slice, HashSet<long>>(SliceComparer.Instance);
                        _dirtyVectorSets[indexedField.Name] = indexedField.VectorIndexer.DirtyNodeIds;
                    }
                }

                if (indexedField.Textual.Count == 0)
                    continue;

                using (staticFieldScope.For(CommitOperation.TextualValues))
                {
                    using var inserter = new TextualFieldInserter(this, indexedField, workingBuffer);
                    inserter.InsertTextualField(token);
                }

                using (staticFieldScope.For(CommitOperation.IntegerValues))
                {
                    using var inserter = new NumericalFieldInserter<long, Int64LookupKey>(this, indexedField, workingBuffer);
                    inserter.InsertNumericalField(token);
                }

                using (staticFieldScope.For(CommitOperation.FloatingValues))
                {
                    using var inserter = new NumericalFieldInserter<double, DoubleLookupKey>(this, indexedField, workingBuffer);
                    inserter.InsertNumericalField(token);
                }

                using (staticFieldScope.For(CommitOperation.SpatialValues))
                    InsertSpatialField(entriesToSpatialTree, indexedField, token);

                if (indexedField.HasMultipleTermsPerField)
                {
                    RecordFieldHasMultipleTerms(indexedField);
                }
            }

            using (stats.For(CommitOperation.StoredValues))
                WriteIndexEntries();

            _pForEncoder.Dispose();
            _pForEncoder = null;

            PersistVectorRootPages();

            // Check if we have suggestions to deal with. 
            if (_hasSuggestions)
            {
                using var __ = stats.For(CommitOperation.Suggestions);
                for (var fieldId = 0; fieldId < _knownFieldsTerms.Length; fieldId++)
                {
                    IndexedField indexedField = _knownFieldsTerms[fieldId];

                    // If there are no suggestion to add, we can continue
                    if (indexedField.Suggestions == null)
                        continue;

                    Slice.From(_entriesAllocator, $"{Constants.IndexWriter.SuggestionsTreePrefix}{fieldId}", out var treeName);

                    var tree = _transaction.CompactTreeFor(treeName);
                    foreach (var (key, counter) in indexedField.Suggestions)
                    {
                        if (tree.TryGetValue(key, out var storedCounter) == false)
                            storedCounter = 0;

                        long finalCounter = storedCounter + counter;
                        if (finalCounter > 0)
                            tree.Add(key, finalCounter);
                        else
                            tree.TryRemove(key, out storedCounter);
                    }
                }
            }

            ArrayPool<IndexedField>.Shared.Return(sortedFieldsBuffer, true);
            // ReSharper disable once RedundantAssignment
            sortedFieldsBuffer = null;

            if (_ownsTransaction)
            {
                _transaction.Commit();
            }
        }

        private void RecordFieldHasMultipleTerms(IndexedField indexedField)
        {
            var tree = _transaction.CreateTree(Constants.IndexWriter.MultipleTermsInField);
            tree.Add(indexedField.Name, 1);
        }

        private void AppendDocumentsBoost()
        {
            _boostedDocs.Sort();
            foreach (var (entryId, documentBoost) in _boostedDocs)
            {
                using var __ = _documentBoost.DirectAdd((long)entryId, out _, out byte* boostPtr);
                float* floatBoostPtr = (float*)boostPtr;
                *floatBoostPtr = documentBoost;
            }
        }

        private void WriteIndexEntries()
        {
            using var writer = new EntryTermsWriter(_entriesAllocator);

            var termsPerEntryId = _termsPerEntryId.ToSpan();
            var termsPerEntryIds = _termsPerEntryIds.ToSpan();

            for (int i = 0; i < _termsPerEntryId.Count; i++)
            {
                ref var termsRef = ref termsPerEntryId[i];
                if (termsRef.Count == 0)
                    continue;

                int size = writer.Encode(termsRef);

                ContainerEntryId entryTermsId = Container.Allocate(_transaction.LowLevelTransaction, _entriesTermsContainerId, size, out var space);
                writer.Write(space);

                _entryIdToLocation.Add((long)termsPerEntryIds[i], (long)entryTermsId);
            }
        }

        private void InsertSpatialField(Tree entriesToSpatialTree, IndexedField indexedField, CancellationToken token)
        {
            if (indexedField.Spatial == null)
                return;

            var fieldRootPage = _fieldsTree.GetLookupRootPage(indexedField.Name);
            Debug.Assert(fieldRootPage != Constants.IndexWriter.InvalidPageId);
            var termContainerId = fieldRootPage << 3 | 0b010;
            Debug.Assert(termContainerId >>> 3 == fieldRootPage, "field root too high?");
            var entriesToTerms = entriesToSpatialTree.FixedTreeFor(indexedField.Name, sizeof(double) + sizeof(double));


            foreach (var (entry, spatialEntry) in indexedField.Spatial)
            {
                token.ThrowIfCancellationRequested();
                spatialEntry.Locations.Sort();

                ref var entryTerms = ref GetEntryTerms(spatialEntry.TermsPerEntryIndex);
                var locations = CollectionsMarshal.AsSpan(spatialEntry.Locations);
                foreach (var item in locations)
                {
                    var (lat, lng) = item;
                    var recordedTerm = new RecordedTerm
                    (
                        termContainerId: termContainerId,
                        lat: lat,
                        lng: lng
                    );

                    if (entryTerms.TryAdd(recordedTerm) == false)
                    {
                        entryTerms.Grow(_entriesAllocator, 1);
                        entryTerms.AddUnsafe(recordedTerm);
                    }
                }

                {
                    var (lat, lng) = locations[0];
                    using (entriesToTerms.DirectAdd(entry, out _, out var ptr))
                    {
                        Unsafe.WriteUnaligned(ptr, lat);
                        Unsafe.WriteUnaligned(ptr + sizeof(double), lng);
                    }
                }
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ref NativeList<RecordedTerm> GetEntryTerms(int termsPerEntryIndex)
        {
            return ref _termsPerEntryId[termsPerEntryIndex];
        }

        private enum AddEntriesToTermResult
        {
            NothingToDo,
            UpdateTermId,
            RemoveTermId,
        }

        /// <param name="idInTree">encoded</param>
        /// <param name="termId">encoded</param>
        private AddEntriesToTermResult AddEntriesToTerm(Span<byte> tmpBuf, long idInTree, bool isNullTerm, ref EntriesModifications entries, out long termId)
        {
            if ((idInTree & (long)TermIdMask.PostingList) != 0)
            {
                return AddEntriesToTermResultViaLargePostingList(ref entries, out termId, isNullTerm, idInTree & Constants.StorageMask.ContainerType);
            }

            if ((idInTree & (long)TermIdMask.SmallPostingList) != 0)
            {
                return AddEntriesToTermResultViaSmallPostingList(tmpBuf, ref entries, out termId, idInTree & Constants.StorageMask.ContainerType);
            }

            return AddEntriesToTermResultSingleValue(tmpBuf, idInTree, ref entries, out termId);
        }

        private AddEntriesToTermResult AddEntriesToTermResultViaSmallPostingList(Span<byte> tmpBuf, ref EntriesModifications entries, out long termIdInTree, long idInTree)
        {
            var containerId = EntryIdEncodings.GetContainerId(idInTree);

            var llt = _transaction.LowLevelTransaction;
            Container.GetMutable(llt, containerId, out var item);

            Debug.Assert(entries.Removals.ToSpan().ToArray().Distinct().Count() == entries.Removals.Count, $"Removals list is not distinct.");


            // combine with existing values

            // PERF: We use SkipLocalsInit because we don't need to ensure this stack space to be filled with zeroes
            // which diminish the amount of work this method has to do.

            var count = VariableSizeEncoding.Read<int>(item.Address, out var offset);
            int capacity = Math.Max(256, count + entries.Additions.Count + entries.Removals.Count);
            _smallPostingListWorkingBuffer.EnsureCapacityFor(capacity);
            _pforDecoder.Init(item.Address + offset, item.Length - offset);
            Debug.Assert(_smallPostingListWorkingBuffer.Capacity > 0 && _smallPostingListWorkingBuffer.Capacity % 256 == 0, "The buffer must be multiple of 256 for PForDecoder.REad");
            _smallPostingListWorkingBuffer.Count = _pforDecoder.Read(_smallPostingListWorkingBuffer.RawItems, _smallPostingListWorkingBuffer.Capacity);
            entries.GetEncodedAdditionsAndRemovals(_entriesAllocator, out long* additions, out long* removals);

            // Merging between existing, additions and removals, there is one scenario where we can just concat the lists together
            // if we have no removals and all of the new additions are *after* the existing ones. Since everything is sorted, this is
            // a very cheap check.
            // existing: [ 10 .. 20 ], removals: [], additions: [ 30 .. 40 ], so result should be [ 10 .. 40 ]
            // In all other scenarios, we have to sort and remove duplicates & removals
            var needSorting = entries.Removals.Count > 0 || // any removal force sorting
                                                            // here we test if the first new addition is smaller than the largest existing, requiring sorting  
                              (entries.Additions.Count > 0 && additions[0] <= _smallPostingListWorkingBuffer.RawItems[_smallPostingListWorkingBuffer.Count - 1]);

            _smallPostingListWorkingBuffer.AddRange(new ReadOnlySpan<long>(additions, entries.Additions.Count));
            _smallPostingListWorkingBuffer.AddRange(new ReadOnlySpan<long>(removals, entries.Removals.Count));

            if (needSorting)
            {
                PostingList.SortEntriesAndRemoveDuplicatesAndRemovals(ref _smallPostingListWorkingBuffer);
            }

            if (_smallPostingListWorkingBuffer.Count == 0)
            {
                Container.Delete(llt, _postingListContainerId, containerId);
                termIdInTree = Constants.IndexSearcher.InvalidId;
                return AddEntriesToTermResult.RemoveTermId;
            }


            if (TryEncodingToBuffer(_smallPostingListWorkingBuffer.RawItems, _smallPostingListWorkingBuffer.Count, tmpBuf, out var encoded) == false)
            {
                AddNewTermToSet(out termIdInTree);
                return AddEntriesToTermResult.UpdateTermId;
            }

            if (encoded.Length == item.Length)
            {
                var mutableSpace = item.ToSpan();
                encoded.CopyTo(mutableSpace);

                // can update in place
                termIdInTree = Constants.IndexSearcher.InvalidId;
                return AddEntriesToTermResult.NothingToDo;
            }

            Container.Delete(llt, _postingListContainerId, containerId);

            termIdInTree = AllocatedSpaceForSmallSet(encoded, llt, out Span<byte> space);

            encoded.CopyTo(space);

            return AddEntriesToTermResult.UpdateTermId;
        }

        private long AllocatedSpaceForSmallSet(Span<byte> encoded, LowLevelTransaction llt, out Span<byte> space)
        {
            // Allocate returns storage-level ContainerEntryId
            ContainerEntryId termIdInTree = Container.Allocate(llt, _postingListContainerId, encoded.Length, out space);

            // Encode for storage: cast ContainerEntryId to long
            return EntryIdEncodings.Encode((long)termIdInTree, 0, TermIdMask.SmallPostingList);
        }

        private AddEntriesToTermResult AddEntriesToTermResultSingleValue(Span<byte> tmpBuf, long idInTree, ref EntriesModifications entries, out long termId)
        {
            entries.AssertPreparationIsNotFinished();

            // Decode returns document-layer ID and frequency from encoded storage value
            var (existingEntryId, existingFrequency) = EntryIdEncodings.Decode(idInTree);

            // In case when existingEntryId and only addition is the same:
            // Let's assert whether the current document will output the same ID as the previous one.
            // We can assume that removals are "agnostic" for us since the already stored document has the same ID as this one.
            // In any other case, where did the different ID come from?
            var additions = entries.Additions.ToSpan();
            if (entries.Additions.Count == 1)
            {
                ref var single = ref additions[0];
                if (single.EntryId == existingEntryId)
                {
                    Debug.Assert(entries.Removals.Count == 0 || entries.Removals.ToSpan()[0].EntryId == existingEntryId);

                    var newId = EntryIdEncodings.Encode(single.EntryId, single.Frequency, TermIdMask.Single);
                    if (newId == idInTree)
                    {
                        termId = Constants.IndexSearcher.InvalidId;
                        return AddEntriesToTermResult.NothingToDo;
                    }
                }
            }

            if (entries.Additions.Count == 0 && entries.Removals.Count > 0)
            {
                if (entries.Removals.Count > 1)
                {
                    ThrowMoreThanOneRemovalFoundForSingleItem(idInTree, entries, existingEntryId, existingFrequency);
                }

                Debug.Assert(EntryIdEncodings.QuantizeAndDequantize(entries.Removals[0].Frequency) == existingFrequency,
                    "The item stored and the item we're trying to delete are different, which is impossible.");

                termId = Constants.IndexSearcher.InvalidId;
                return AddEntriesToTermResult.RemoveTermId;
            }

            // Another document contains the same term. Let's check if the currently indexed document is in EntriesModification.
            // If it's not, we have to add it (since it has to be included in Small/Set too).
            if (entries.Additions.Count >= 1)
            {
                bool isIncluded = false;
                for (int idX = 0; idX < entries.Additions.Count && isIncluded == false; ++idX)
                {
                    if (entries.Additions[idX].EntryId == existingEntryId)
                        isIncluded = true;
                }

                //User may want to delete it.
                for (int idX = 0; idX < entries.Removals.Count && isIncluded == false; ++idX)
                {
                    if (entries.Removals[idX].EntryId == existingEntryId)
                        isIncluded = true;
                }

                if (isIncluded == false)
                {
                    // We are not processing recorded terms for this document because it already exists on the disk. We do not have to know the actual term type.
                    entries.Addition(_entriesAllocator, existingEntryId, -1, existingFrequency, InserterMode.Ignore);
                }
            }


            CreatePostingListForNewTerm(ref entries, tmpBuf, out termId);
            return AddEntriesToTermResult.UpdateTermId;
        }

        /// <summary>
        /// Operation to perform on a lookup tree after processing a term.
        /// </summary>
        /// <param name="Operation">Operation to perform.</param>
        /// <param name="TermId">Encoded location of the posting list / single document.</param>
        private record struct LookupTreeOperationJob(AddEntriesToTermResult Operation, long TermId);

        private AddEntriesToTermResult AddEntriesToTermResultViaLargePostingList(ref EntriesModifications entries, out long termId, bool isNullTerm, long id)
        {
            var containerId = EntryIdEncodings.GetContainerId(id);
            var llt = _transaction.LowLevelTransaction;
            var setSpace = Container.GetMutable(llt, containerId);
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);

            entries.GetEncodedAdditionsAndRemovals(_entriesAllocator, out var additions, out var removals);

            var numberOfEntries = PostingList.Update(_transaction.LowLevelTransaction, ref postingListState, additions, entries.Additions.Count, removals,
                entries.Removals.Count, _pForEncoder, ref _tempListBuffer, ref _pforDecoder);

            termId = Constants.IndexSearcher.InvalidId;

            if (numberOfEntries == 0)
            {
                if (isNullTerm) // we don't want to remove the null term posting list
                    return AddEntriesToTermResult.NothingToDo;

                llt.FreePage(postingListState.RootPage);

                Container.Delete(llt, _postingListContainerId, containerId);
                RemovePostingListFromLargePostingListsSet((long)containerId);

                return AddEntriesToTermResult.RemoveTermId;
            }

            return AddEntriesToTermResult.NothingToDo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void RemovePostingListFromLargePostingListsSet(long containerId)
        {
            _largePostingListSet ??= _transaction.OpenPostingList(Constants.IndexWriter.LargePostingListsSetSlice);
            _largePostingListSet.Remove(containerId);
        }

        private bool TryEncodingToBuffer(long* additions, int additionsCount, Span<byte> tmpBuf, out Span<byte> encoded)
        {
            fixed (byte* pOutput = tmpBuf)
            {
                var offset = VariableSizeEncoding.Write(pOutput, additionsCount);

                var size = _pForEncoder.Encode(additions, additionsCount);
                if (size >= tmpBuf.Length - offset)
                {
                    encoded = default;
                    return false;
                }

                (int count, int sizeUsed) = _pForEncoder.Write(pOutput + offset, tmpBuf.Length - offset);
                Debug.Assert(count == additionsCount);
                Debug.Assert(sizeUsed == size);

                encoded = tmpBuf[..(size + offset)];
                return true;
            }
        }

        /// <summary>
        /// Create a new posting list or just encode entryId (in case when a term is unique).
        /// </summary>
        /// <param name="entries">Term's entries</param>
        /// <param name="tmpBuf">Working buffer</param>
        /// <param name="termId">Location of stored entries / encoded entry id</param>
        private void CreatePostingListForNewTerm(ref EntriesModifications entries, Span<byte> tmpBuf, out long termId)
        {
            _numberOfTermModifications += 1;
            Debug.Assert(entries.Additions.Count > 0, "entries.TotalAdditions > 0");
            // common for unique values (guid, date, etc)
            if (entries.Additions.Count == 1)
            {
                entries.AssertPreparationIsNotFinished();
                ref var single = ref entries.Additions.ToSpan()[0];
                termId = EntryIdEncodings.Encode(single.EntryId, single.Frequency, TermIdMask.Single);
                return;
            }

            entries.GetEncodedAdditionsAndRemovals(_entriesAllocator, out var additions, out _);
            if (TryEncodingToBuffer(additions, entries.Additions.Count, tmpBuf, out var encoded) == false)
            {
                // too big, convert to a set
                AddNewTermToSet(out termId);
                return;
            }

            termId = AllocatedSpaceForSmallSet(encoded, _transaction.LowLevelTransaction, out Span<byte> space);
            encoded.CopyTo(space);
        }

        private void AddNewTermToSet(out long termId)
        {
            long setId = (long)Container.Allocate(_transaction.LowLevelTransaction, _postingListContainerId, sizeof(PostingListState), out var setSpace);

            // we need to account for the size of the posting lists, once a term has been switch to a posting list
            // it will always be in this model, so we don't need to do any cleanup
            _largePostingListSet ??= _transaction.OpenPostingList(Constants.IndexWriter.LargePostingListsSetSlice);
            _largePostingListSet.Add(setId);

            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);
            PostingList.Create(_transaction.LowLevelTransaction, ref postingListState, _pForEncoder);
            termId = EntryIdEncodings.Encode(setId, 0, TermIdMask.PostingList);
        }

        public void Dispose()
        {
            _compactKeyScope.Dispose();
            _termsPerEntryId.Dispose(_entriesAllocator);
            _termsPerEntryIds.Dispose(_entriesAllocator);
            _pforDecoder.Dispose();
            _entriesAllocator.Dispose();
            _jsonOperationContext?.Dispose();

            foreach (var indexedField in _knownFieldsTerms)
                indexedField.VectorIndexer?.Dispose();

            foreach (var (_, indexedField) in _dynamicFieldsTerms)
                indexedField.VectorIndexer?.Dispose();

            if (_ownsTransaction)
                _transaction?.Dispose();

            _analyzersContext.Dispose();
            _indexDebugDumper.Dispose();
            _entryBuilder.Clean();
        }

        public void ReduceModificationCount()
        {
            _numberOfModifications--;
        }
    }
}
