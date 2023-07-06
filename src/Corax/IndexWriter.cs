using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Server;
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
using InvalidOperationException = System.InvalidOperationException;

namespace Corax
{
    // container ids are guaranteed to be aligned on 
    // 4 bytes boundary, we're using this to store metadata
    // about the data
    [Flags]
    public enum TermIdMask : long
    {
        Single = 0,
        
        EnsureIsSingleMask = 0b11,
        
        SmallPostingList = 1,
        PostingList = 2
    }

    public partial class IndexWriter : IDisposable // single threaded, controlled by caller
    {
        private long _numberOfModifications;
        private readonly Dictionary<ByteString, EntriesContainer> _indexedEntriesByKey = new(new ByteStringEqualityComparer());
        private readonly List<Slice> _entryKeysToRemove = new();
        private readonly IndexFieldsMapping _fieldsMapping;
        private FixedSizeTree _documentBoost;
        private Tree _indexMetadata;
        private Tree _persistedDynamicFieldsAnalyzers;
        private long _numberOfTermModifications;

        private struct EntriesContainer
        {
            public long SingleItem;
            public List<long> Items;
        }
        
        private readonly bool _ownsTransaction;
        private JsonOperationContext _jsonOperationContext;
        private readonly Transaction _transaction;

        private Token[] _tokensBufferHandler;
        private byte[] _encodingBufferHandler;
        private byte[] _utf8ConverterBufferHandler;

        internal unsafe struct EntriesModifications : IDisposable
        {
            private readonly ByteStringContext _context;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _memoryHandler;

            public long? Long;
            public double? Double;

            private long* _start;
            private long* _end;
            private int _additions;
            private int _removals;

            public int TermSize => _sizeOfTerm;
            private readonly int _sizeOfTerm;


            private short* _freqStart;
            private short* _freqEnd;
            private bool _needToSortToDeleteDuplicates;

#if DEBUG
            private int _hasChangesCallCount = 0;
            private bool _preparationFinished = false;
#endif

            public bool HasChanges()
            {
                AssertHasChangesIsCalledOnlyOnce();

                DeleteAllDuplicates();

                return _removals != 0 || _additions != 0;
            }

            public int TotalAdditions => _additions;
            public int TotalRemovals => _removals;

            public long TotalSpace => _end - _start;
            public long FreeSpace => TotalSpace - (_additions + _removals);

            public ReadOnlySpan<long> Additions => new(_start, _additions);

            public long* RawAdditions => _start;

            public ReadOnlySpan<long> Removals => new(_end - _removals, _removals);

            public ReadOnlySpan<short> AdditionsFrequency => new(_freqStart, _additions);

            public ReadOnlySpan<short> RemovalsFrequency => new(_freqEnd - _removals, _removals);

            private const int InitialSize = 16;

            // Entries Writer structure:
            // [ADDITIONS][REMOVALS][ADDITIONS_FREQUENCY][REMOVALS_FREQUENCY]
            // We've to track additions and removals independently since we encode frequencies inside entryId in inverted index.
            // Reason:
            // In case of posting lists we've to remove old encoded entry id from list when doing frequency update, to do so we've to options:
            // iterate through all entries and encode them in-place to find which one to delete or just seek to exact key.
            // To seek in place we store frequency during building data for Commit. This gives us possibility to encode old key
            // and call "remove".
            public EntriesModifications([NotNull] ByteStringContext context, int size)
            {
                _sizeOfTerm = size;
                _context = context;
                _memoryHandler = _context.Allocate(CalculateSizeOfContainer(InitialSize), out var output);
                _needToSortToDeleteDuplicates = true;
                _start = (long*)output.Ptr;
                _end = _start + InitialSize;
                _freqStart = (short*)_end;
                _freqEnd = _freqStart + InitialSize;
                new Span<short>(_freqStart, InitialSize).Fill(1);
                _additions = 0;
                _removals = 0;
            }

            private static int CalculateSizeOfContainer(int size) => size * (sizeof(long) + sizeof(short));

            public void Addition(long entryId, short freq = 1)
            {
                AssertPreparationIsNotFinished();
                _needToSortToDeleteDuplicates = true;

                if (_additions > 0 && *(_start + _additions - 1) == entryId)
                {
                    ref var frequency = ref *(_freqStart + _additions - 1);
                    if (frequency < short.MaxValue)
                        frequency++;
                    return;
                }

                if (FreeSpace == 0)
                    GrowBuffer();

                *(_start + _additions) = entryId;
                _additions++;
            }

            public void Removal(long entryId)
            {
                AssertPreparationIsNotFinished();
                _needToSortToDeleteDuplicates = true;

                if (_removals > 0 && *(_end - _removals) == entryId)
                {
                    ref var frequency = ref *(_freqEnd - _removals);
                    if (frequency < short.MaxValue)
                        frequency++;
                    return;
                }

                if (FreeSpace == 0)
                    GrowBuffer();

                _removals++;
                *(_end - _removals) = entryId;
            }

            private void GrowBuffer()
            {
                int totalSpace = (int)TotalSpace;
                int newTotalSpace = totalSpace * 2;

                var itemsScope = _context.Allocate(CalculateSizeOfContainer(newTotalSpace), out var itemsOutput);

                long* start = (long*)itemsOutput.Ptr;
                long* end = start + newTotalSpace;

                short* freqStart = (short*)end;
                short* freqEnd = freqStart + newTotalSpace;

                // Copy the contents that we already have.
                Unsafe.CopyBlockUnaligned(start, _start, (uint)_additions * sizeof(long));
                Unsafe.CopyBlockUnaligned(end - _removals, _end - _removals, (uint)_removals * sizeof(long));

                //CopyFreq
                Unsafe.CopyBlockUnaligned(freqStart, _freqStart, (uint)_additions * sizeof(short));
                Unsafe.CopyBlockUnaligned(freqEnd - _removals, _freqEnd - _removals, (uint)_removals * sizeof(short));

                //All new items are 1 by default
                new Span<short>(freqStart + _additions, newTotalSpace - (_additions + _removals)).Fill(1);


#if DEBUG
                var additionsBufferEqual = new Span<long>(_start, _additions).SequenceEqual(new Span<long>(start, _additions));
                var removalsBufferEqual = new Span<long>(_end - _removals, _removals).SequenceEqual(new Span<long>(end - _removals, _removals));
                var additionsFrequencyBufferEqual = new Span<short>(_freqStart, _additions).SequenceEqual(new Span<short>(freqStart, _additions));
                var removalsFrequencyBufferEqual = new Span<short>(_freqEnd - _removals, _removals).SequenceEqual(new Span<short>(freqEnd - _removals, _removals));
                if ((additionsBufferEqual && removalsBufferEqual && additionsFrequencyBufferEqual && removalsFrequencyBufferEqual) == false)
                    throw new InvalidDataException($"Lost item(s) in {nameof(GrowBuffer)}." +
                                                   $"{Environment.NewLine}Additions buffer equal: {additionsBufferEqual}" +
                                                   $"{Environment.NewLine}Removals buffer equal: {removalsBufferEqual}" +
                                                   $"{Environment.NewLine}Additions frequency buffer equal: {additionsFrequencyBufferEqual}" +
                                                   $"{Environment.NewLine}Removals frequency buffer equal: {removalsFrequencyBufferEqual}");
#endif

                // Return the memory
                _memoryHandler.Dispose();
                _freqStart = freqStart;
                _freqEnd = freqEnd;
                _start = start;
                _end = end;
                _memoryHandler = itemsScope;
            }

            private void DeleteAllDuplicates()
            {
                if (_needToSortToDeleteDuplicates == false)
                    return;
                _needToSortToDeleteDuplicates = false;

                var additions = new Span<long>(_start, _additions);
                var freqAdditions = new Span<short>(_freqStart, _additions);
                var removals = new Span<long>(_end - _removals, _removals);
                var freqRemovals = new Span<short>(_freqEnd - _removals, _removals);


                MemoryExtensions.Sort(additions, freqAdditions);
                MemoryExtensions.Sort(removals, freqRemovals);


                int duplicatesFound = 0;
                for (int add = 0, rem = 0; add < additions.Length && rem < removals.Length; ++add)
                {
                    Start:
                    ref var currentAdd = ref additions[add];
                    ref var currentRemoval = ref removals[rem];
                    var currentFreqAdd = freqAdditions[add];
                    var currentFreqRemove = freqRemovals[rem];

                    //We've to delete exactly same item in additions and removals and delete those.
                    //This is made for Set structure.
                    if (currentAdd == currentRemoval && currentFreqAdd == currentFreqRemove)
                    {
                        currentRemoval = -1;
                        currentAdd = -1;
                        duplicatesFound++;
                        rem++;
                        continue;
                    }

                    if (currentAdd < currentRemoval)
                        continue;

                    if (currentAdd > currentRemoval)
                    {
                        rem++;
                        while (rem < removals.Length)
                        {
                            if (currentAdd <= removals[rem])
                                goto Start;
                            rem++;
                        }
                    }
                }

                if (duplicatesFound != 0)
                {
                    // rare case
                    MemoryExtensions.Sort(additions, freqAdditions);
                    MemoryExtensions.Sort(removals, freqRemovals);
                    _additions -= duplicatesFound;
                    _removals -= duplicatesFound;

                    //Moving memory
                    additions.Slice(duplicatesFound).CopyTo(additions);
                    freqAdditions.Slice(duplicatesFound).CopyTo(freqAdditions);

                    removals.Slice(duplicatesFound).CopyTo(new Span<long>(_end - _removals, _removals));
                    freqRemovals.Slice(duplicatesFound).CopyTo(new Span<short>(_freqEnd - _removals, _removals));
                    ValidateNoDuplicateEntries();
                }
            }

            // There is an case we found in RavenDB-19688
            // Sometimes term can be added and removed for the same in the same batch and there can be multiple other docs between this two operations.
            // This requires us to ensure we don't have duplicates here.
            public void PrepareDataForCommiting()
            {
#if DEBUG
                if (_preparationFinished)
                    throw new InvalidOperationException(
                        $"{nameof(PrepareDataForCommiting)} should be called only once. This is a bug. It was called via: {Environment.NewLine}" +
                        Environment.StackTrace);
                _preparationFinished = true;
#endif


                DeleteAllDuplicates();

                EntryIdEncodings.Encode(new Span<long>(_start, _additions), new Span<short>(_freqStart, _additions));
                EntryIdEncodings.Encode(new Span<long>(_end - _removals, _removals), new Span<short>(_freqEnd - _removals, _removals));
            }

            [Conditional("DEBUG")]
            private void ValidateNoDuplicateEntries()
            {
                var removals = Removals;
                var additions = Additions;
                foreach (var add in additions)
                {
                    if (removals.BinarySearch(add) >= 0)
                        throw new InvalidOperationException("Found duplicate addition & removal item during indexing: " + add);
                }

                foreach (var removal in removals)
                {
                    if (additions.BinarySearch(removal) >= 0)
                        throw new InvalidOperationException("Found duplicate addition & removal item during indexing: " + removal);
                }
            }

            [Conditional("DEBUG")]
            public void AssertPreparationIsNotFinished()
            {
#if DEBUG
                if (_preparationFinished)
                    throw new InvalidOperationException("Tried to Add/Remove but data is already encoded.");
#endif
            }

            [Conditional("DEBUG")]
            private void AssertHasChangesIsCalledOnlyOnce()
            {
#if DEBUG
                _hasChangesCallCount++;
                if (_hasChangesCallCount > 1)
                    throw new InvalidOperationException($"{nameof(HasChanges)} should be called only once.");
#endif
            }

            public void Dispose()
            {
                _memoryHandler.Dispose();
            }
        }

        private class IndexedField
        {
            public Dictionary<long, List<(double, double)>> Spatial;
            public readonly Dictionary<Slice, EntriesModifications> Textual;
            public readonly Dictionary<long, EntriesModifications> Longs;
            public readonly Dictionary<double, EntriesModifications> Doubles;
            public Dictionary<Slice, int> Suggestions;
            public readonly Analyzer Analyzer;
            public readonly Slice Name;
            public readonly Slice NameLong;
            public readonly Slice NameDouble;
            public readonly Slice NameTotalLengthOfTerms;
            public readonly int Id;
            public readonly FieldIndexingMode FieldIndexingMode;
            public readonly bool HasSuggestions;
            public readonly bool ShouldStore;

            public IndexedField(IndexFieldBinding binding) : this(binding.FieldId, binding.FieldName, binding.FieldNameLong, binding.FieldNameDouble,
                binding.FieldTermTotalSumField, binding.Analyzer, binding.FieldIndexingMode, binding.HasSuggestions, binding.ShouldStore)
            {
            }

            public IndexedField(int id, Slice name, Slice nameLong, Slice nameDouble, Slice nameTotalLengthOfTerms, Analyzer analyzer,
                FieldIndexingMode fieldIndexingMode, bool hasSuggestions, bool shouldStore)
            {
                Name = name;
                NameLong = nameLong;
                NameDouble = nameDouble;
                NameTotalLengthOfTerms = nameTotalLengthOfTerms;
                Id = id;
                Analyzer = analyzer;
                HasSuggestions = hasSuggestions;
                ShouldStore = shouldStore;
                Textual = new Dictionary<Slice, EntriesModifications>(SliceComparer.Instance);
                Longs = new Dictionary<long, EntriesModifications>();
                Doubles = new Dictionary<double, EntriesModifications>();
                FieldIndexingMode = fieldIndexingMode;
            }

        }

        private bool _hasSuggestions;
        private readonly IndexedField[] _knownFieldsTerms;
        private Dictionary<Slice, IndexedField> _dynamicFieldsTerms;
        private readonly HashSet<long> _deletedEntries = new();
        private FieldsCache _fieldsCache;

        private long _postingListContainerId;
        private long _storedFieldsContainerId;
        private long _entriesTermsContainerId;
        private Lookup<Int64LookupKey> _entryIdToLocation;
        private IndexFieldsMapping _dynamicFieldsMapping;
        private PostingList _largePostingListSet;

        public void UpdateDynamicFieldsMapping(IndexFieldsMapping current)
        {
            _dynamicFieldsMapping = current;
        }

        private const string SuggestionsTreePrefix = "__Suggestion_";

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index writer with opening semantics and also every new
        // writer becomes essentially a unit of work which makes reusing assets tracking more explicit.

        private IndexWriter(IndexFieldsMapping fieldsMapping)
        {
            _indexDebugDumper = new IndexOperationsDumper(fieldsMapping);
            _builder = new IndexEntryBuilder(this);
            _fieldsMapping = fieldsMapping;
            _encodingBufferHandler = Analyzer.BufferPool.Rent(fieldsMapping.MaximumOutputSize);
            _tokensBufferHandler = Analyzer.TokensPool.Rent(fieldsMapping.MaximumTokenSize);
            _utf8ConverterBufferHandler = Analyzer.BufferPool.Rent(fieldsMapping.MaximumOutputSize * 10);

            var bufferSize = fieldsMapping!.Count;
            _knownFieldsTerms = new IndexedField[bufferSize];
            for (int i = 0; i < bufferSize; ++i)
                _knownFieldsTerms[i] = new IndexedField(fieldsMapping.GetByFieldId(i));

            _entriesAlreadyAdded = new HashSet<long>();
            _additionsForTerm = new List<long>();
            _removalsForTerm = new List<long>();
        }

        public IndexWriter([NotNull] StorageEnvironment environment, IndexFieldsMapping fieldsMapping) : this(fieldsMapping)
        {
            TransactionPersistentContext transactionPersistentContext = new(true);
            _transaction = environment.WriteTransaction(transactionPersistentContext);

            _ownsTransaction = true;
            Init();
        }

        private void Init()
        {
            _postingListContainerId = _transaction.OpenContainer(Constants.IndexWriter.PostingListsSlice);
            _storedFieldsContainerId = _transaction.OpenContainer(Constants.IndexWriter.StoreFieldsSlice);
            _entriesTermsContainerId = _transaction.OpenContainer(Constants.IndexWriter.EntriesTermsContainerSlice);
            _entryIdToLocation = _transaction.LookupFor<Int64LookupKey>(Constants.IndexWriter.EntryIdToLocationSlice);
            _jsonOperationContext = JsonOperationContext.ShortTermSingleUse();
            _persistedDynamicFieldsAnalyzers = _transaction.CreateTree(Constants.IndexWriter.DynamicFieldsAnalyzersSlice);

            _indexMetadata = _transaction.CreateTree(Constants.IndexMetadataSlice);
            _initialNumberOfEntries = _indexMetadata?.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0;
            _lastEntryId =  _indexMetadata?.ReadInt64(Constants.IndexWriter.LastEntryIdSlice) ?? 0;

            _documentBoost = _transaction.FixedTreeFor(Constants.DocumentBoostSlice, sizeof(float));
            _entriesAllocator = new ByteStringContext(SharedMultipleUseFlag.None);
        }
        public IndexWriter([NotNull] Transaction tx, IndexFieldsMapping fieldsMapping) : this(fieldsMapping)
        {
            _transaction = tx;

            _ownsTransaction = false;
            Init();
        }

        public long Index(string key, ReadOnlySpan<byte> data, float documentBoost)
        {
            return Index(Encoding.UTF8.GetBytes(key), data, documentBoost);
        }

        public long Index(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data, float documentBoost)
        {
            var idInTree = Index(key, data);
            AppendDocumentBoost(EntryIdEncodings.DecodeAndDiscardFrequency(idInTree), documentBoost);
            return idInTree;
        }

        public class IndexEntryBuilder : IDisposable
        {
            private IndexWriter _parent;
            private long _entryId;
            private ByteStringContext _context;
            public bool Active;
            public bool IsEmpty => Fields > 0;
            public int Fields;

            public IndexEntryBuilder(IndexWriter parent)
            {
                _parent = parent;
                _context = new ByteStringContext(SharedMultipleUseFlag.None);
            }

            public void Init(long entryId)
            {
                Active = true;
                Fields = 0;
                _entryId = entryId;
                _context.Reset();
            }

            public void Dispose()
            {
                Active = false;
            }

            public void WriteNull(int fieldId, string path)
            {
                ExactInsert(GetField(fieldId, path), Constants.NullValueSlice);
            }

            private IndexedField GetField(int fieldId, string path)
            {
                Fields++;

                var field = fieldId != Constants.IndexWriter.DynamicField ? _parent._knownFieldsTerms[fieldId] : _parent.GetDynamicIndexedField(_context, path);
                return field;
            }

            void Insert(IndexedField field, ReadOnlySpan<byte> value)
            {
                if (field.Analyzer != null)
                    AnalyzeInsert(field, value);
                else
                    ExactInsert(field, value);
            }

            void AnalyzeInsert(IndexedField field, ReadOnlySpan<byte> value)
            {
                var analyzer = field.Analyzer;
                if (value.Length > _parent._encodingBufferHandler.Length)
                {
                    analyzer.GetOutputBuffersSize(value.Length, out var outputSize, out var tokenSize);
                    if (outputSize > _parent._encodingBufferHandler.Length || tokenSize > _parent._tokensBufferHandler.Length)
                        _parent.UnlikelyGrowAnalyzerBuffer(outputSize, tokenSize);
                }

                Span<byte> wordsBuffer = _parent._encodingBufferHandler;
                Span<Token> tokens = _parent._tokensBufferHandler;
                analyzer.Execute(value, ref wordsBuffer, ref tokens, ref _parent._utf8ConverterBufferHandler);

                for (int i = 0; i < tokens.Length; i++)
                {
                    ref var token = ref tokens[i];

                    if (token.Offset + token.Length > _parent._encodingBufferHandler.Length)
                        _parent.ThrowInvalidTokenFoundOnBuffer(field, value, wordsBuffer, tokens, token);

                    var word = new Span<byte>(_parent._encodingBufferHandler, token.Offset, (int)token.Length);
                    ExactInsert(field, word);
                }
            }

            ref EntriesModifications ExactInsert(IndexedField field, ReadOnlySpan<byte> value)
            {
                ByteStringContext<ByteStringMemoryCache>.InternalScope? scope = CreateNormalizedTerm(_context, value, out var slice);

                // We are gonna try to get the reference if it exists, but we wont try to do the addition here, because to store in the
                // dictionary we need to close the slice as we are disposing it afterwards. 
                ref var term = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Textual, slice, out var exists);
                if (exists == false)
                {
                    term = new EntriesModifications(_context, value.Length);
                    scope = null; // We don't want the fieldname (slice) to be returned.
                }

                term.Addition(_entryId);

                if (field.HasSuggestions)
                    _parent.AddSuggestions(field, slice);

                scope?.Dispose();

                return ref term;
            }

            private void RecordSpatialPointForEntry(IndexedField field, (double Lat, double Lng) coords)
            {
                field.Spatial ??= new();
                ref var terms = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Spatial, _entryId, out var exists);
                if (exists == false)
                {
                    terms = new List<(double, double)>();
                }

                terms.Add(coords);
            }

            internal void Clean()
            {
                _context.Dispose();
            }

            public void Write(int fieldId, string path, ReadOnlySpan<byte> value)
            {
                var field = GetField(fieldId, path);
                Insert(field, value);
            }

            public void Write(int fieldId, string path, ReadOnlySpan<byte> value, long longValue, double dblValue)
            {
                var field = GetField(fieldId, path);
                Insert(field, value);
            }

            public void WriteSpatial(int fieldId, string path, CoraxSpatialPointEntry entry)
            {
                var field = GetField(fieldId, path);
                RecordSpatialPointForEntry(field, (entry.Latitude, entry.Longitude));

                var maxLen = Encoding.UTF8.GetMaxByteCount(path.Length);
                using var _ = _context.Allocate(maxLen, out var buffer);
                var len = Encoding.UTF8.GetBytes(entry.Geohash, buffer.ToSpan());
                for (int i = 1; i <= entry.Geohash.Length; ++i)
                {
                    ExactInsert(field, buffer.ToReadOnlySpan()[..i]);
                }
            }

            public void Store(BlittableJsonReaderObject storedValue)
            {
                var field = _parent._knownFieldsTerms[^1];
                RegisterTerm(field.Name, storedValue.AsSpan(), StoredFieldType.None);
            }

            public void Store(int fieldId, string name, ReadOnlySpan<byte> storedValue)
            {
                var field = GetField(fieldId, name);
                RegisterTerm(field.Name, storedValue, StoredFieldType.Raw);
            }


            void RegisterTerm(Slice fieldName, ReadOnlySpan<byte> term, StoredFieldType type)
            {
                ref var entryTerms = ref _parent.GetEntryTerms(_entryId);
                var fieldsTree = _parent._transaction.CreateTree(Constants.IndexWriter.FieldsSlice);
                long fieldRootPage = _parent._fieldsCache.GetFieldRootPage(fieldName, fieldsTree);

                var termId = Container.Allocate(_parent._transaction.LowLevelTransaction, _parent._storedFieldsContainerId,
                    term.Length, fieldRootPage, out Span<byte> space);
                term.CopyTo(space);
                entryTerms.Add(new RecordedTerm
                {
                    // why: entryTerms.Count << 8 
                    // we put entries count here because we are sorting the entries afterward
                    // this ensure that stored values are then read using the same order we have for writing them
                    // which is important for storing arrays
                    TermContainerId = entryTerms.Count << 8 | (int)type | 0b110, // marker for stored field
                    Long = termId
                });
            }

            public void Index()
            {
                
            }
        }

        private readonly IndexEntryBuilder _builder;

        public IndexEntryBuilder Index(string key)
        {
            return Index(Encoding.UTF8.GetBytes(key)); 
        }

        public IndexEntryBuilder Index(ReadOnlySpan<byte> key)
        {
            if (_builder.Active)
                throw new NotSupportedException("You *must* dispose the previous builder before calling it again");
            _builder.Active = true;
            return _builder;
        }

        public long Index(string key, ReadOnlySpan<byte> data)
        {
            return Index(Encoding.UTF8.GetBytes(key), data);
        }
        /// <returns>Encoded id (with freq/container_type)</returns>
        public long Index(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data)
        {
            _numberOfModifications++;
            var entryId = ++_lastEntryId;
            _entriesAllocator.Allocate(key.Length, out var keyStr);
            key.CopyTo(keyStr.ToSpan());
            
            IndexEntry(entryId, data);

            ref var container = ref CollectionsMarshal.GetValueRefOrAddDefault(_indexedEntriesByKey, keyStr, out var exists);
            if (exists == false)
            {
                container.SingleItem = entryId;
            }
            else if(container.Items == null)
            {
                container.Items = new List<long>
                {
                    container.SingleItem, 
                    entryId,
                };
            }
            else
            {
                container.Items.Add(entryId);
            }
            
            return EntryIdEncodings.Encode(entryId, 1, TermIdMask.Single);
        }

        private unsafe void IndexEntry(long entryId, ReadOnlySpan<byte> data)
        {
            var context = _transaction.Allocator;
            fixed (byte* newEntryDataPtr = data)
            {
                var entryReader = new IndexEntryReader(newEntryDataPtr, data.Length);

                foreach (var binding in _fieldsMapping)
                {
                    var fieldReader = entryReader.GetFieldReaderFor(binding.FieldId);
                    if (binding.ShouldStore)
                    {
                        StoreField(entryId, ref fieldReader, binding.FieldName);
                    }
                    
                    if (binding.FieldIndexingMode is FieldIndexingMode.No)
                        continue;

                    var indexer = new TermIndexer(this, context, fieldReader, _knownFieldsTerms[binding.FieldId], entryId);
                    indexer.InsertToken();
                }

                var it = new IndexEntryReader.DynamicFieldEnumerator(entryReader);
                while (it.MoveNext())
                {
                    var fieldReader = entryReader.GetFieldReaderFor(it.CurrentFieldName);

                    var indexedField = GetDynamicIndexedField(context, it.CurrentFieldName);
                    if (indexedField.ShouldStore)
                    {
                        StoreField(entryId, ref fieldReader, indexedField.Name);
                    }
                    if (indexedField.FieldIndexingMode is FieldIndexingMode.No)
                        continue;


                    var indexer = new TermIndexer(this, context, fieldReader, indexedField, entryId);
                    indexer.InsertToken();
                }
            }
        }

        private void StoreField(long entryId, ref IndexEntryReader.FieldReader fieldReader, Slice name)
        {
            var fieldsTree = _transaction.CreateTree(Constants.IndexWriter.FieldsSlice);
            long fieldRootPage = _fieldsCache.GetFieldRootPage(name, fieldsTree);
            ref var entryTerms = ref GetEntryTerms(entryId);

            if (fieldReader.Type.HasFlag(IndexEntryFieldType.List))
            {
                if (fieldReader.TryReadMany(out var it))
                {
                    while (it.ReadNext())
                    {
                        if (it.IsNull)
                        {
                            RegisterNullOrEmpty(ref entryTerms, StoredFieldType.Null);
                        }
                        else if (it.IsEmptyString)
                        {
                            RegisterNullOrEmpty(ref entryTerms, StoredFieldType.Empty);
                        }
                        else
                        {
                            RegisterTerm(it.Sequence, ref entryTerms, fieldReader.Type);
                        }
                    }
                }
            }
            else if (fieldReader.Read(out var type, out Span<byte> v))
            {
                switch (type)
                {
                    case IndexEntryFieldType.Null:
                        RegisterNullOrEmpty(ref entryTerms, StoredFieldType.Null);
                        break;
                    case IndexEntryFieldType.Empty:
                        RegisterNullOrEmpty(ref entryTerms, StoredFieldType.Empty);
                        break;
                    default:
                        RegisterTerm(v, ref entryTerms, fieldReader.Type);
                        break;
                }
            }

            void RegisterTerm(ReadOnlySpan<byte> term, ref NativeList<RecordedTerm> entryTerms, IndexEntryFieldType type)
            {
                var termId = Container.Allocate(_transaction.LowLevelTransaction, _storedFieldsContainerId,
                    term.Length, fieldRootPage, out Span<byte> space);
                term.CopyTo(space);
                var storedFieldType = StoredFieldType.Term;
                if (type.HasFlag(IndexEntryFieldType.Raw))
                {
                    storedFieldType = StoredFieldType.Raw;
                }
                if (type.HasFlag(IndexEntryFieldType.List))
                {
                    storedFieldType |= StoredFieldType.List;
                }
                if (type.HasFlag(IndexEntryFieldType.Tuple))
                {
                    storedFieldType |= StoredFieldType.Tuple;
                }
                entryTerms.Add(new RecordedTerm
                {
                    // why: entryTerms.Count << 8 
                    // we put entries count here because we are sorting the entries afterward
                    // this ensure that stored values are then read using the same order we have for writing them
                    // which is important for storing arrays
                    TermContainerId = entryTerms.Count << 8 | (int)storedFieldType | 0b110, // marker for stored field
                    Long = termId
                });
            }

            void RegisterNullOrEmpty(ref NativeList<RecordedTerm> entryTerms, StoredFieldType type)
            {
                entryTerms.Add(new RecordedTerm
                {
                    TermContainerId = entryTerms.Count << 8 |  (int)type | 0b110, // marker stored field entry
                    Long = fieldRootPage
                });
            }
        }

        //Document Boost should add priority to some documents but also should not be the main component of boosting.
        //The natural logarithm slows down our scoring increase for a document so that the ranking calculated at query time is not forgotten.
        //We've to add entry container id (without frequency etc) here because in 'SortingMatch' we have already decoded ids.
        private unsafe void AppendDocumentBoost(long entryId, float documentBoost, bool isUpdate = false)
        {
            if (documentBoost.AlmostEquals(1f))
            {
                
                // We don't store `1` but if user update boost value to 1 we've to delete the previous one
                if (isUpdate)
                    _documentBoost.Delete(entryId);
                
                return;
            }

            // probably user want this to be at the same end.
            if (documentBoost <= 0f)
                documentBoost = 0;

            documentBoost = MathF.Log(documentBoost + 1); // ensure we've positive number
            
            using var __ = _documentBoost.DirectAdd(entryId, out _, out byte* boostPtr);
            float* floatBoostPtr = (float*)boostPtr;
            *floatBoostPtr = documentBoost;
        }


        /// <param name="entryId">Container id of entry (without encodings)</param>
        private void RemoveDocumentBoost(long entryId)
        {
            _documentBoost.Delete(entryId);
        }
        
        public long Update(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data, float documentBoost)
        {
            var idInTree = Update(key,  data);
            AppendDocumentBoost(EntryIdEncodings.DecodeAndDiscardFrequency(idInTree), documentBoost, true);
            return idInTree;
        }

        /// <returns>Encoded entryId</returns>
        public long Update(ReadOnlySpan<byte> key, ReadOnlySpan<byte> data)
        {
            var scope = Slice.From(_transaction.Allocator, key, ByteStringType.Mutable, out var slice);
            if (_indexedEntriesByKey.Remove(slice.Content, out var existing))
            {
                // if this is already in the index batch, we know that it was already marked for deletion
                // from persistent storage, so we can just remove it from the batch and be done with it
                RemoveEntriesFromBatch(ref existing);
                scope.Dispose();
            }
            else
            {
                _entryKeysToRemove.Add(slice);
            }
            return Index(key, data);
        }

        private IndexedField GetDynamicIndexedField(ByteStringContext context, string currentFieldName)
        {
            using var _ = Slice.From(context, currentFieldName, out var slice);
            return GetDynamicIndexedField(context, slice);
        }
        
        private IndexedField GetDynamicIndexedField(ByteStringContext context, Span<byte> currentFieldName)
        {
            using var _ = Slice.From(context, currentFieldName, out var slice);
            return GetDynamicIndexedField(context, slice);
        }

        private IndexedField GetDynamicIndexedField(ByteStringContext context, Slice slice)
        {
            if (_fieldsMapping.TryGetByFieldName(slice, out var indexFieldBinding))
                return _knownFieldsTerms[indexFieldBinding.FieldId];

            _dynamicFieldsTerms ??= new(SliceComparer.Instance);
            if (_dynamicFieldsTerms.TryGetValue(slice, out var indexedField))
                return indexedField;

            var clonedFieldName = slice.Clone(context);

            if (_dynamicFieldsMapping is null || _persistedDynamicFieldsAnalyzers is null)
            {
                CreateDynamicField(null, FieldIndexingMode.Normal);
                return indexedField;
            }

            var persistedAnalyzer = _persistedDynamicFieldsAnalyzers.Read(slice);
            if (_dynamicFieldsMapping?.TryGetByFieldName(slice, out var binding) is true)
            {
                indexedField = new IndexedField(Constants.IndexWriter.DynamicField, binding.FieldName, binding.FieldNameLong,
                    binding.FieldNameDouble, binding.FieldTermTotalSumField, binding.Analyzer,
                    binding.FieldIndexingMode, binding.HasSuggestions, binding.ShouldStore);

                if (persistedAnalyzer != null)
                {
                    var originalIndexingMode = (FieldIndexingMode)persistedAnalyzer.Reader.ReadByte();
                    if (binding.FieldIndexingMode != originalIndexingMode)
                        throw new InvalidDataException(
                            $"Inconsistent dynamic field creation options were detected. Field '{binding.FieldName}' was created with '{originalIndexingMode}' analyzer but now '{binding.FieldIndexingMode}' analyzer was specified. This is not supported");
                }

                if (binding.FieldIndexingMode != FieldIndexingMode.Normal && persistedAnalyzer == null)
                {
                    _persistedDynamicFieldsAnalyzers.Add(slice, (byte)binding.FieldIndexingMode);
                }

                _dynamicFieldsTerms[clonedFieldName] = indexedField;
            }
            else
            {
                FieldIndexingMode mode;
                if (persistedAnalyzer == null)
                {
                    mode = FieldIndexingMode.Normal;
                }
                else
                {
                    mode = (FieldIndexingMode)persistedAnalyzer.Reader.ReadByte();
                }

                Analyzer analyzer = mode switch
                {
                    FieldIndexingMode.No => null,
                    FieldIndexingMode.Exact => _dynamicFieldsMapping!.ExactAnalyzer(slice.ToString()),
                    FieldIndexingMode.Search => _dynamicFieldsMapping!.SearchAnalyzer(slice.ToString()),
                    _ => _dynamicFieldsMapping!.DefaultAnalyzer
                };

                CreateDynamicField(analyzer, mode);
            }

            return indexedField;

            void CreateDynamicField(Analyzer analyzer, FieldIndexingMode mode)
            {
                IndexFieldsMappingBuilder.GetFieldNameForLongs(context, clonedFieldName, out var fieldNameLong);
                IndexFieldsMappingBuilder.GetFieldNameForDoubles(context, clonedFieldName, out var fieldNameDouble);
                IndexFieldsMappingBuilder.GetFieldForTotalSum(context, clonedFieldName, out var nameSum);
                indexedField = new IndexedField(Constants.IndexWriter.DynamicField, clonedFieldName, fieldNameLong, fieldNameDouble, nameSum, analyzer, mode, false, false);
                _dynamicFieldsTerms[clonedFieldName] = indexedField;
            }
        }

        private long _initialNumberOfEntries;
        private readonly HashSet<long> _entriesAlreadyAdded;
        private readonly List<long> _additionsForTerm, _removalsForTerm;
        private readonly IndexOperationsDumper _indexDebugDumper;
        private long _lastEntryId;
        private FastPForEncoder _pForEncoder;
        private Dictionary<long, NativeList<RecordedTerm>> _termsPerEntryId = new();
        private ByteStringContext _entriesAllocator;

        [StructLayout(LayoutKind.Explicit, Pack = 1)]
        public struct RecordedTerm : IComparable<RecordedTerm>
        {
            [FieldOffset(0)]
            public long TermContainerId;
            [FieldOffset(8)]
            public long Long;
            [FieldOffset(16)]
            public double Double;

            [FieldOffset(8)]
            public double Lat;
            [FieldOffset(16)]
            public double Lng;
            
            // We take advantage of the fact that container ids always have the bottom bits cleared
            // to store metadata information here. Otherwise, we'll pay extra 8 bytes per term
            public bool HasLong => (TermContainerId & 1) == 1;
            public bool HasDouble => (TermContainerId & 2) == 2;

            public int CompareTo(RecordedTerm other)
            {
                return TermContainerId.CompareTo(other.TermContainerId);
            }
        }
        
        public long GetNumberOfEntries() => _initialNumberOfEntries + _numberOfModifications;

        private void AddSuggestions(IndexedField field, Slice slice)
        {
            _hasSuggestions = true;
            field.Suggestions ??= new Dictionary<Slice, int>();
            
            var keys = SuggestionsKeys.Generate(_transaction.Allocator, Constants.Suggestions.DefaultNGramSize, slice.AsSpan(), out int keysCount);
            int keySizes = keys.Length / keysCount;

            var bsc = _transaction.Allocator;

            var suggestionsToAdd = field.Suggestions;

            int idx = 0;
            while (idx < keysCount)
            {
                var key = new Slice(bsc.Slice(keys, idx * keySizes, keySizes, ByteStringType.Immutable));
                if (suggestionsToAdd.TryGetValue(key, out int counter) == false)
                    counter = 0;

                counter++;
                suggestionsToAdd[key] = counter;
                idx++;
            }
        }

        private void RemoveSuggestions(IndexedField field, ReadOnlySpan<byte> sequence)
        {
            _hasSuggestions = true;
            field.Suggestions ??= new Dictionary<Slice, int>();


            var keys = SuggestionsKeys.Generate(_transaction.Allocator, Constants.Suggestions.DefaultNGramSize, sequence, out int keysCount);
            int keySizes = keys.Length / keysCount;

            var bsc = _transaction.Allocator;
            var suggestionsToAdd = field.Suggestions;

            int idx = 0;
            while (idx < keysCount)
            {
                var key = new Slice(bsc.Slice(keys, idx * keySizes, keySizes, ByteStringType.Immutable));
                if (suggestionsToAdd.TryGetValue(key, out int counter) == false)
                    counter = 0;

                counter--;
                suggestionsToAdd[key] = counter;
                idx++;
            }
        }

        private readonly ref struct TermIndexer
        {
            private readonly IndexEntryReader.FieldReader _fieldReader;
            private readonly IndexWriter _parent;
            
            /// <summary>
            /// Id of index entry from container. We use it to build `EntriesModifications` so it has to be without frequency etc (because it will be added later). 
            /// </summary>
            private readonly long _entryId;
            
            private readonly ByteStringContext _context;
            private readonly IndexedField _indexedField;


            public TermIndexer(IndexWriter parent, ByteStringContext context, IndexEntryReader.FieldReader fieldReader, IndexedField indexedField, long entryId)
            {
                _fieldReader = fieldReader;
                _parent = parent;
                _entryId = entryId;
                _context = context;
                _indexedField = indexedField;
            }

#if !DEBUG
            [SkipLocalsInit]
#endif
            public void InsertToken()
            {
                switch (_fieldReader.Type)
                {
                    case IndexEntryFieldType.Empty:
                    case IndexEntryFieldType.Null:
                        var fieldName = _fieldReader.Type == IndexEntryFieldType.Null ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                        ExactInsert(fieldName.AsReadOnlySpan());
                        break;

                    case IndexEntryFieldType.TupleListWithNulls:
                    case IndexEntryFieldType.TupleList:
                        if (_fieldReader.TryReadMany(out var iterator) == false)
                            break;
                        
                        while (iterator.ReadNext())
                        {
                            if (iterator.IsNull)
                            {
                                ref var modifications = ref ExactInsert(Constants.NullValueSlice);
                                modifications.Long = 0;
                                modifications.Double = double.NaN;
                                NumericInsert(0L, double.NaN);
                            }
                            else if (iterator.IsEmptyString)
                            {
                                throw new InvalidDataException("Tuple list cannot contain an empty string (otherwise, where did the numeric came from!)");
                            }
                            else
                            {
                                ref var modifications = ref ExactInsert(iterator.Sequence);
                                modifications.Long = iterator.Long;
                                modifications.Double = iterator.Double;
                                NumericInsert(iterator.Long, iterator.Double);
                            }
                        }

                        break;

                    case IndexEntryFieldType.Tuple:
                    {
                        if (_fieldReader.Read(out _, out long lVal, out double dVal, out Span<byte> valueInEntry) == false)
                            break;

                        ref var modifications = ref ExactInsert(valueInEntry);
                        modifications.Long = lVal;
                        modifications.Double = dVal;
                        NumericInsert(lVal, dVal);
                        break;
                    }
                    case IndexEntryFieldType.SpatialPointList:
                        if (_fieldReader.TryReadManySpatialPoint(out var spatialIterator) == false)
                            break;

                        while (spatialIterator.ReadNext())
                        {
                            RecordSpatialPointForEntry((spatialIterator.Latitude, spatialIterator.Longitude));

                            for (int i = 1; i <= spatialIterator.Geohash.Length; ++i)
                                ExactInsert(spatialIterator.Geohash.Slice(0, i));
                        }

                        break;

                    case IndexEntryFieldType.SpatialPoint:
                    {
                        if (_fieldReader.Read(out var valueInEntry) == false)
                            break;

                        RecordSpatialPointForEntry(_fieldReader.ReadSpatialPoint());

                        for (int i = 1; i <= valueInEntry.Length; ++i)
                            ExactInsert(valueInEntry.Slice(0, i));

                        break;
                    }
                    case IndexEntryFieldType.ListWithNulls:
                    case IndexEntryFieldType.List:
                        if (_fieldReader.TryReadMany(out iterator) == false)
                            break;

                        while (iterator.ReadNext())
                        {
                            Debug.Assert((_fieldReader.Type & IndexEntryFieldType.Tuple) == 0, "(fieldType & IndexEntryFieldType.Tuple) == 0");

                            if (iterator.IsNull || iterator.IsEmptyString)
                            {
                                var fieldValue = iterator.IsNull ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                                ExactInsert(fieldValue.AsReadOnlySpan());
                            }
                            else
                            {
                                 Insert(iterator.Sequence);
                            }
                        }

                        break;
                    case IndexEntryFieldType.Raw:
                    case IndexEntryFieldType.RawList:
                    case IndexEntryFieldType.Invalid:
                        break;
                    default:
                        if (_fieldReader.Read(out var value) == false)
                            break;

                        Insert(value);
                        break;
                }
            }

            private void RecordSpatialPointForEntry((double Lat, double Lng) coords)
            {
                _indexedField.Spatial ??= new();
                ref var terms = ref CollectionsMarshal.GetValueRefOrAddDefault(_indexedField.Spatial, _entryId, out var exists);
                if (exists == false)
                {
                    terms = new List<(double, double)>();
                }
                terms.Add(coords);
            }

            void NumericInsert(long lVal, double dVal)
            {
                // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
                ref var doublesTerms = ref CollectionsMarshal.GetValueRefOrAddDefault(_indexedField.Doubles, dVal, out bool fieldDoublesExist);
                if (fieldDoublesExist == false)
                    doublesTerms = new EntriesModifications(_parent._transaction.Allocator, sizeof(double));
                doublesTerms.Addition(_entryId);

                // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
                ref var longsTerms = ref CollectionsMarshal.GetValueRefOrAddDefault(_indexedField.Longs, lVal, out bool fieldLongExist);
                if (fieldLongExist == false)
                    longsTerms = new EntriesModifications(_parent._transaction.Allocator, sizeof(long));
                longsTerms.Addition(_entryId);
            }


            void Insert(ReadOnlySpan<byte> value)
            {
                if (_indexedField.Analyzer != null)
                    AnalyzeInsert(value);
                else
                    ExactInsert(value);
            }

            void AnalyzeInsert(ReadOnlySpan<byte> value)
            {
                var analyzer = _indexedField.Analyzer;
                if (value.Length > _parent._encodingBufferHandler.Length)
                {
                    analyzer.GetOutputBuffersSize(value.Length, out var outputSize, out var tokenSize);
                    if (outputSize > _parent._encodingBufferHandler.Length || tokenSize > _parent._tokensBufferHandler.Length)
                        _parent.UnlikelyGrowAnalyzerBuffer(outputSize, tokenSize);
                }

                Span<byte> wordsBuffer = _parent._encodingBufferHandler;
                Span<Token> tokens = _parent._tokensBufferHandler;
                analyzer.Execute(value, ref wordsBuffer, ref tokens, ref _parent._utf8ConverterBufferHandler);

                for (int i = 0; i < tokens.Length; i++)
                {
                    ref var token = ref tokens[i];

                    if (token.Offset + token.Length > _parent._encodingBufferHandler.Length)
                        _parent.ThrowInvalidTokenFoundOnBuffer(_indexedField, value, wordsBuffer, tokens, token);

                    var word = new Span<byte>(_parent._encodingBufferHandler, token.Offset, (int)token.Length);
                    ExactInsert(word);
                }

            }

            ref EntriesModifications ExactInsert(ReadOnlySpan<byte> value)
            {
                ByteStringContext<ByteStringMemoryCache>.InternalScope? scope = CreateNormalizedTerm(_context, value, out var slice);
                
                // We are gonna try to get the reference if it exists, but we wont try to do the addition here, because to store in the
                // dictionary we need to close the slice as we are disposing it afterwards. 
                ref var term = ref CollectionsMarshal.GetValueRefOrAddDefault(_indexedField.Textual, slice, out var exists);
                if (exists == false)
                {
                    term = new EntriesModifications(_context, value.Length);
                    scope = null; // We don't want the fieldname (slice) to be returned.
                }

                term.Addition(_entryId);

                if (_indexedField.HasSuggestions)
                    _parent.AddSuggestions(_indexedField, slice);

                scope?.Dispose();

                return ref term;
            }
        }
        
        private static unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope CreateNormalizedTerm(ByteStringContext context, ReadOnlySpan<byte> value,
            out Slice slice)
        {
            ulong hash = 0;
            int length = value.Length;
            if (length > Constants.Terms.MaxLength)
            {
                int hashStartingPoint = Constants.Terms.MaxLength - 2 * sizeof(ulong);
                hash = Hashing.XXHash64.Calculate(value.Slice(hashStartingPoint));

                Span<byte> localValue = stackalloc byte[Constants.Terms.MaxLength];
                value.Slice(0, Constants.Terms.MaxLength).CopyTo(localValue);
                int hexSize = Numbers.FillAsHex(localValue.Slice(hashStartingPoint), hash);
                Debug.Assert(Constants.Terms.MaxLength == hashStartingPoint + hexSize, "Constants.Terms.MaxLength == hashStartingPoint + hexSize");

                return Slice.From(context, localValue, ByteStringType.Mutable, out slice);
            }
            else
            {
                return Slice.From(context, value, ByteStringType.Mutable, out slice);
            }
        }

        private void ProcessDeletes(Tree fieldsTree) 
        {
            // we need to copy the keys so we could sort them
            Sorter<Slice, SliceStructComparer> sorter = default;
            sorter.Sort(CollectionsMarshal.AsSpan(_entryKeysToRemove));
            
            // now run over the keys in sorted fashion in optimal manner
            var primaryKeyTree = fieldsTree.CompactTreeFor(_fieldsMapping.GetByFieldId(0).FieldName);
            primaryKeyTree.InitializeStateForTryGetNextValue();
            for (int i = 0; i < _entryKeysToRemove.Count; i++)
            {
                if (primaryKeyTree.TryGetNextValue(_entryKeysToRemove[i].AsSpan(), out var _, out var postingListId, out _, out var scope))
                {
                    RecordDeletion(postingListId);
                    scope.Dispose();
                }
            }
            
            var llt = _transaction.LowLevelTransaction;
            Page lastVisitedPage = default;

            var fieldsByRootPage = GetIndexedFieldByRootPage(fieldsTree);
            
            long dicId = CompactTree.GetDictionaryId(llt);
            var compactKey = llt.AcquireCompactKey();
            
            foreach (long entryToDelete in _deletedEntries)
            {
                if (_entryIdToLocation.TryRemove(entryToDelete, out var entryTermsId) == false)
                    throw new InvalidOperationException("Unable to locate entry id: " + entryToDelete);

                RemoveDocumentBoost(entryToDelete);
                var entryTerms = Container.MaybeGetFromSamePage(llt, ref lastVisitedPage, entryTermsId);
                RecordTermDeletionsForEntry(entryTerms, llt, fieldsByRootPage, compactKey, dicId, entryToDelete);
            }
        }

        private unsafe void RecordTermDeletionsForEntry(Container.Item entryTerms, LowLevelTransaction llt, Dictionary<long, IndexedField> fieldsByRootPage, CompactKey compactKey, long dicId,
            long entryToDelete)
        {
            var reader = new EntryTermsReader(llt, entryTerms.Address, entryTerms.Length, dicId);
            reader.Reset();
            while (reader.MoveNextStoredField())
            {
                Container.Delete(llt, _storedFieldsContainerId, reader.TermId);
            }
            reader.Reset();
            while (reader.MoveNext())
            {
                if (fieldsByRootPage.TryGetValue(reader.FieldRootPage, out var field) == false)
                {
                    throw new InvalidOperationException($"Unable to find matching field for {reader.FieldRootPage} with root page:  {reader.FieldRootPage}. Term: '{reader.Current}'");
                }
                
                var ptr = reader.Current.DecodedPtr(out var len);
                var scope = Slice.From(llt.Allocator, ptr, len, out Slice termSlice);
                if(field.HasSuggestions)
                    RemoveSuggestions(field, new ReadOnlySpan<byte>(ptr, len));
                
                ref var term = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Textual, termSlice, out var exists);
                if (exists == false)
                {
                    term = new EntriesModifications(llt.Allocator, len);
                    scope = default; // We dont want to reclaim the term name
                }

                //TODO: Maciej - we need to modify EntriesModifications to accept it externally 
                for (int i = 0; i < reader.Frequency; i++)
                {
                    term.Removal(entryToDelete);
                }
                scope.Dispose();
                
                if(reader.HasNumeric == false)
                    continue;
                
                term = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Longs, reader.CurrentLong, out exists);
                if (exists == false)
                {
                    term = new EntriesModifications(llt.Allocator, sizeof(long));
                }

                term.Removal(entryToDelete);
                
                term = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Doubles, reader.CurrentDouble, out exists);
                if (exists == false)
                {
                    term = new EntriesModifications(llt.Allocator, sizeof(long));
                }

                term.Removal(entryToDelete);
            }
        }

        private unsafe Dictionary<long, IndexedField> GetIndexedFieldByRootPage(Tree fieldsTree)
        {
            var pageToField = new Dictionary<long, IndexedField>();
            var it = fieldsTree.Iterate(prefetch: false);
            if (it.Seek(Slices.BeforeAllKeys))
            {
                do
                {
                    var state = (LookupState*)it.CreateReaderForCurrent().Base;
                    if (state->RootObjectType == RootObjectType.Lookup)
                    {
                        var found = _fieldsMapping.TryGetByFieldName(it.CurrentKey, out var field);
                        if (found == false)
                        {
                            if(it.CurrentKey.EndsWith("-D"u8) || it.CurrentKey.EndsWith("-L"u8))
                                continue; // numeric postfix values
                            var dynamicIndexedField = GetDynamicIndexedField(_transaction.Allocator, it.CurrentKey.AsSpan());
                            pageToField.Add(state->RootPage, dynamicIndexedField);
                        }
                        else
                        {
                            pageToField.Add(state->RootPage, _knownFieldsTerms[field.FieldId]);
                        }
                    }
                } while (it.MoveNext());
            }

            return pageToField;
        }
        
        public void TryDeleteEntry(string term)
        {
            var scope = Slice.From(_transaction.Allocator, term, ByteStringType.Immutable, out var termSlice);

            if (_indexedEntriesByKey.Remove(termSlice.Content, out var existing))
            {
                // if this is already in the index batch, we know that it was already marked for deletion
                // from persistent storage, so we can just remove it from the batch and be done with it
                RemoveEntriesFromBatch(ref existing);
                scope.Dispose();
                return;
            }
            
            _entryKeysToRemove.Add(termSlice);
        }
        
        public void TryDeleteEntryByField(string field, string term)
        {
            using var _ = Slice.From(_transaction.Allocator, term, ByteStringType.Immutable, out var termSlice);
            using var __ = Slice.From(_transaction.Allocator, field, ByteStringType.Immutable, out var fieldSlice);
            if (TryGetEntryTermId(fieldSlice, termSlice.AsSpan(), out long idInTree) == false) 
                return;

            RecordDeletion(idInTree);
        }
        
        private void RemoveEntriesFromBatch(ref EntriesContainer entriesContainer)
        {
            throw new NotImplementedException();
            // not bother with _returning_ the data, since this is part of a batch (will be cleaned in the end)
            // and we assume that this is rare, we want to use pointer bumping for
            // allocations as much as possible
            // if (entriesContainer.Items != null)
            // {
            //     foreach (long item in entriesContainer.Items)
            //     {
            //         _bufferedIndexedEntries.Remove(item);
            //     }
            // }
            // else
            // {
            //     _bufferedIndexedEntries.Remove(entriesContainer.SingleItem);
            // }
        }
        
        /// <summary>
        /// Record term for deletion from Index.
        /// </summary>
        /// <param name="idInTree">With frequencies and container type.</param>
        [SkipLocalsInit]
        private unsafe void RecordDeletion(long idInTree)
        {
            var containerId = EntryIdEncodings.GetContainerId(idInTree);
            if ((idInTree & (long)TermIdMask.PostingList) != 0)
            {
                var setSpace = Container.GetMutable(_transaction.LowLevelTransaction, containerId);
                ref var setState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);
                
                using var set = new PostingList(_transaction.LowLevelTransaction, Slices.Empty, setState);
                var iterator = set.Iterate();
                Span<long> buffer = stackalloc long[1024];
                while (iterator.Fill(buffer, out var read))
                {
                    // since this is also encoded we've to delete frequency and container type as well
                    EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);
                    for (int i = 0; i < read; i++)
                    {
                        long entryId = buffer[i];
                        _deletedEntries.Add(entryId);
                    }
                    _numberOfModifications -= read;
                }
            }
            else if ((idInTree & (long)TermIdMask.SmallPostingList) != 0)
            {
                var smallSet = Container.Get(_transaction.LowLevelTransaction, containerId);
                // combine with existing value
                _ = VariableSizeEncoding.Read<int>(smallSet.Address, out var pos);
                
                var decoder = new FastPForDecoder(_transaction.Allocator, smallSet.Address + pos, smallSet.Length - pos);
                var output = stackalloc long[1024];
                while (true)
                {
                    var read = decoder.Read(output, 1024);
                    if (read == 0) 
                        break;
                    EntryIdEncodings.DecodeAndDiscardFrequency(new Span<long>(output, 1024), read);
                    for (int i = 0; i < read; i++)
                    {
                        long entryId = output[i];
                        _deletedEntries.Add(entryId);
                    }
                    _numberOfModifications -= read;
                }
                decoder.Dispose();
            }
            else
            {
                _deletedEntries.Add(containerId);
                _numberOfModifications--;
            }
        }

        /// <summary>
        /// Get TermId (id of container) from FieldTree 
        /// </summary>
        /// <param name="idInTree">Has frequency and container type inside idInTree.</param>
        /// <returns></returns>
        private bool TryGetEntryTermId(Slice fieldName, ReadOnlySpan<byte> term, out long idInTree)
        {
            var fieldsTree = _transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
            if (fieldsTree == null)
            {
                idInTree = -1;
                return false;
            }

            var fieldTree = fieldsTree.CompactTreeFor(fieldName);

            // We need to normalize the term in case we have a term bigger than MaxTermLength.
            using var __ = CreateNormalizedTerm(_transaction.Allocator, term, out var termSlice);

            var termValue = termSlice.AsReadOnlySpan();
            return fieldTree.TryGetValue(termValue, out idInTree);
        }

        public void PrepareAndCommit()
        {
            Prepare();
            Commit();
        }

        public void Prepare()
        {
           
        }

        public void Commit()
        {
            _indexDebugDumper.Commit();
            using var _ = _transaction.Allocator.Allocate(Container.MaxSizeInsideContainerPage, out Span<byte> workingBuffer);
            Tree fieldsTree = _transaction.CreateTree(Constants.IndexWriter.FieldsSlice);
            
            ProcessDeletes(fieldsTree);
            
            Tree entriesToTermsTree = _transaction.CreateTree(Constants.IndexWriter.EntriesToTermsSlice);
            Tree entriesToSpatialTree = _transaction.CreateTree(Constants.IndexWriter.EntriesToSpatialSlice);
            _indexMetadata.Increment(Constants.IndexWriter.NumberOfEntriesSlice, _numberOfModifications);
            _indexMetadata.Increment(Constants.IndexWriter.NumberOfTermsInIndex, _numberOfTermModifications);
            _indexMetadata.Add(Constants.IndexWriter.LastEntryIdSlice, _lastEntryId);
            _pForEncoder = new FastPForEncoder(_transaction.LowLevelTransaction.Allocator);

            Slice[] keys = Array.Empty<Slice>();
            for (int fieldId = 0; fieldId < _fieldsMapping.Count; ++fieldId)
            {
                var indexedField = _knownFieldsTerms[fieldId];
                if (indexedField.Textual.Count == 0)
                    continue;

                InsertTextualField(fieldsTree, entriesToTermsTree, indexedField, workingBuffer, ref keys);
                InsertNumericFieldLongs(fieldsTree, entriesToTermsTree, indexedField, workingBuffer);
                InsertNumericFieldDoubles(fieldsTree, entriesToTermsTree, indexedField, workingBuffer);
                InsertSpatialField(entriesToSpatialTree, fieldsTree, indexedField);
            }

            if (_dynamicFieldsTerms != null)
            {
                foreach (var (_, indexedField) in _dynamicFieldsTerms)
                {
                    InsertTextualField(fieldsTree, entriesToTermsTree, indexedField, workingBuffer, ref keys);
                    InsertNumericFieldLongs(fieldsTree, entriesToTermsTree, indexedField, workingBuffer);
                    InsertNumericFieldDoubles(fieldsTree, entriesToTermsTree, indexedField, workingBuffer);
                    InsertSpatialField(entriesToSpatialTree,  fieldsTree, indexedField);
                }
            }

            WriteIndexEntries();

            _pForEncoder.Dispose();
            _pForEncoder = null;
            
            if(keys.Length>0)
                ArrayPool<Slice>.Shared.Return(keys);

            // Check if we have suggestions to deal with. 
            if (_hasSuggestions)
            {
                for (var fieldId = 0; fieldId < _knownFieldsTerms.Length; fieldId++)
                {
                    IndexedField indexedField = _knownFieldsTerms[fieldId];
                    if (indexedField.Suggestions == null) continue;
                    Slice.From(_transaction.Allocator, $"{SuggestionsTreePrefix}{fieldId}", out var treeName);
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

            if (_ownsTransaction)
            {
                _transaction.Commit();
            }
        }

        private void WriteIndexEntries()
        {
            var writer = new EntryTermsWriter(_transaction.Allocator);
            foreach (var (entry, terms) in _termsPerEntryId)
            {
                int size = writer.Encode(terms);
                long entryTermsId = Container.Allocate(_transaction.LowLevelTransaction, _entriesTermsContainerId, size, out var space);
                writer.Write(space);
                _entryIdToLocation.Add(entry, entryTermsId);
            }
            writer.Dispose();
        }

        private unsafe void InsertSpatialField(Tree entriesToSpatialTree, Tree fieldsTree, IndexedField indexedField)
        {
            if (indexedField.Spatial == null)
                return;

            var fieldRootPage = fieldsTree.GetLookupRootPage(indexedField.Name);
            Debug.Assert(fieldRootPage != -1);
            var termContainerId = fieldRootPage << 3 | 0b010;
            Debug.Assert(termContainerId >>> 3 == fieldRootPage, "field root too high?");
            var entriesToTerms = entriesToSpatialTree.FixedTreeFor(indexedField.Name, sizeof(double)+sizeof(double));
            foreach (var (entry, list)  in indexedField.Spatial)
            {
                list.Sort();

                ref var entryTerms = ref GetEntryTerms(entry);
                for (int i = 0; i < list.Count; i++)
                {
                    var (lat,lng) = list[i];
                    entryTerms.Add(new RecordedTerm
                    {
                        TermContainerId = termContainerId,
                        Lat =  lat,
                        Lng =  lng
                    });
                }

                {
                    var (lat, lng) = list[0];
                    using (entriesToTerms.DirectAdd(entry, out _, out var ptr))
                    {
                        Unsafe.WriteUnaligned(ptr, lat);
                        Unsafe.WriteUnaligned(ptr + sizeof(double), lng);
                    }
                }
            }
        }

        private unsafe void InsertTextualField(Tree fieldsTree, Tree entriesToTermsTree, IndexedField indexedField, Span<byte> tmpBuf, ref Slice[] sortedTermsBuffer)
        {
            var fieldTree = fieldsTree.CompactTreeFor(indexedField.Name);
            var currentFieldTerms = indexedField.Textual;
            int termsCount = currentFieldTerms.Count;
            var entriesToTerms = entriesToTermsTree.LookupFor<Int64LookupKey>(indexedField.Name); 

            _entriesAlreadyAdded.Clear();
         
            if (sortedTermsBuffer.Length < termsCount)
            {
                if (sortedTermsBuffer.Length > 0)
                    ArrayPool<Slice>.Shared.Return(sortedTermsBuffer);
                sortedTermsBuffer = ArrayPool<Slice>.Shared.Rent(termsCount);
            }

            currentFieldTerms.Keys.CopyTo(sortedTermsBuffer, 0);

            // Sorting the terms buffer.
            Sorter<Slice, SliceStructComparer> sorter = default;
            sorter.Sort(sortedTermsBuffer, 0, termsCount);

            using var dumper = new IndexTermDumper(fieldsTree, indexedField.Name);

            fieldTree.InitializeStateForTryGetNextValue();
            long totalLengthOfTerm = 0;

            var newAdditions = new NativeIntegersList(_transaction.Allocator);
            for (var index = 0; index < termsCount; index++)
            {
                var term = sortedTermsBuffer[index];
                ref var entries = ref CollectionsMarshal.GetValueRefOrNullRef(currentFieldTerms, term);
                Debug.Assert(Unsafe.IsNullRef(ref entries) == false);
                if (entries.HasChanges() == false)
                    continue;

                newAdditions.InitCopyFrom(entries.Additions);

                if (indexedField.Spatial == null) // For spatial, we handle this in InsertSpatialField, so we skip it here
                {
                    SetRange(_additionsForTerm, entries.Additions);
                    SetRange(_removalsForTerm, entries.Removals);
                }

                long termId;
                ReadOnlySpan<byte> termsSpan = term.AsSpan();

                bool found = fieldTree.TryGetNextValue(termsSpan, out var termContainerId, out var existingIdInTree, out var keyLookup, out var scope);
                Debug.Assert(found || entries.TotalRemovals == 0, "Cannot remove entries from term that isn't already there");
                if (entries.TotalAdditions > 0 && found == false)
                {
                    if (entries.TotalRemovals != 0)
                        throw new InvalidOperationException($"Attempt to remove entries from new term: '{term}' for field {indexedField.Name}! This is a bug.");

                    AddNewTerm(ref entries, tmpBuf, out termId);
                    totalLengthOfTerm += entries.TermSize;
                    
                    dumper.WriteAddition(term, termId);
                    termContainerId = fieldTree.AddAfterTryGetNext(ref keyLookup, termId);
                }
                else
                {
                    switch (AddEntriesToTerm(tmpBuf, existingIdInTree, ref entries, out termId))
                    {
                        case AddEntriesToTermResult.UpdateTermId:
                            if (termId != existingIdInTree)
                            {
                                dumper.WriteRemoval(term, existingIdInTree);
                            }
                            dumper.WriteAddition(term, termId);
                            fieldTree.SetAfterTryGetNext(ref keyLookup, termId);
                            break;
                        case AddEntriesToTermResult.RemoveTermId:
                            if (fieldTree.TryRemoveExistingValue(ref keyLookup, out var ttt) == false)
                            {
                                dumper.WriteRemoval(term, termId);
                                ThrowTriedToDeleteTermThatDoesNotExists();
                            }

                            totalLengthOfTerm -= entries.TermSize;
                            
                            dumper.WriteRemoval(term, ttt);
                            _numberOfTermModifications--;
                            break;
                        case AddEntriesToTermResult.NothingToDo:
                            break;
                    }
                    
                    void ThrowTriedToDeleteTermThatDoesNotExists()
                    {
                        throw new InvalidOperationException($"Attempt to remove term: '{term}' for field {indexedField.Name}, but it does not exists! This is a bug.");
                    }
                }

                for (int i = 0; i < newAdditions.Count; i++)
                {
                    var entry = newAdditions.RawItems[i];
                    ref var entryTerms = ref GetEntryTerms(entry);

                    Debug.Assert((termContainerId & 0b111) == 0); // ensure that the three bottom bits are cleared
                    var recordedTerm = new RecordedTerm
                    {
                        TermContainerId = entries.AdditionsFrequency[i] switch
                        {
                            > 1 => termContainerId << 8 | // note, bottom 3 are cleared, so we have 11 bits to play with
                                   EntryIdEncodings.FrequencyQuantization(entries.AdditionsFrequency[i]) << 3 |
                                   0b100, // marker indicating that we have a term frequency here
                            _ => termContainerId
                        }
                    };

                    if (entries.Long != null)
                    {
                        recordedTerm.TermContainerId |= 1; // marker!
                        recordedTerm.Long = entries.Long.Value;

                        // only if the double value can not be computed by casting from long, we store it 
                        if (entries.Double!.Value.Equals((double)entries.Long.Value) == false)
                        {
                            recordedTerm.TermContainerId |= 2; // marker!
                            recordedTerm.Double = entries.Double!.Value;
                        }
                    }
                    
                    entryTerms.Add(recordedTerm);
                }
                
                if (indexedField.Spatial == null)
                {
                    Debug.Assert(termContainerId > 0);
                    InsertEntriesForTerm(entriesToTerms, termContainerId);
                }

                scope.Dispose();
            }
            newAdditions.Dispose();
            
            _indexMetadata.Increment(indexedField.NameTotalLengthOfTerms, totalLengthOfTerm);
        }

        private ref NativeList<RecordedTerm> GetEntryTerms(long entry)
        {
            ref var entryTerms = ref CollectionsMarshal.GetValueRefOrAddDefault(_termsPerEntryId, entry, out var exists);
            if (exists == false)
            {
                entryTerms = new NativeList<RecordedTerm>(_transaction.Allocator);
            }
            return ref entryTerms;
        }

        private void SetRange(List<long> list, ReadOnlySpan<long> span)
        {
            list.Clear();
            for (int i = 0; i < span.Length; i++)
            {
                list.Add(span[i]);
            }
        }

        private enum AddEntriesToTermResult
        {
            NothingToDo,
            UpdateTermId,
            RemoveTermId,
        }


        /// <param name="idInTree">encoded</param>
        /// <param name="termId">encoded</param>
        private AddEntriesToTermResult AddEntriesToTerm(Span<byte> tmpBuf, long idInTree, ref EntriesModifications entries, out long termId)
        {
            if ((idInTree & (long)TermIdMask.PostingList) != 0)
            {
                return AddEntriesToTermResultViaLargePostingList(ref entries, out termId, idInTree & Constants.StorageMask.ContainerType);
            }
            if ((idInTree & (long)TermIdMask.SmallPostingList) != 0)
            {
                return AddEntriesToTermResultViaSmallPostingList(tmpBuf, ref entries, out termId, idInTree & Constants.StorageMask.ContainerType);
            }
            return AddEntriesToTermResultSingleValue(tmpBuf, idInTree, ref entries, out termId);
        }

        private unsafe AddEntriesToTermResult AddEntriesToTermResultViaSmallPostingList(Span<byte> tmpBuf, ref EntriesModifications entries, out long termIdInTree, long idInTree)
        {
            var containerId = EntryIdEncodings.GetContainerId(idInTree);
            
            var llt = _transaction.LowLevelTransaction;
            var item = Container.Get(llt, containerId);
            Debug.Assert(entries.Removals.ToArray().Distinct().Count() == entries.TotalRemovals, $"Removals list is not distinct.");
            int removalIndex = 0;
            
            // combine with existing values
            var buffer = stackalloc long[1024];
            var bufferAsSpan = new Span<long>(buffer, 1024);
            _ = VariableSizeEncoding.Read<int>(item.Address, out var offset); // discard count here
            var reader = new FastPForDecoder(_transaction.Allocator,item.Address + offset, item.Length - offset);
            var removals = entries.Removals;
            long freeSpace = entries.FreeSpace;
            while (true)
            {
                var read = reader.Read(buffer, 1024);
                if (read == 0)
                    break;

                EntryIdEncodings.DecodeAndDiscardFrequency(bufferAsSpan, read);
                for (int i = 0; i < read; i++)
                {
                    if (removalIndex < removals.Length)
                    {
                        if (buffer[i] == removals[removalIndex])
                        {
                            removalIndex++;
                            continue;
                        }

                        if (buffer[i] > removals[removalIndex])
                            throw new InvalidDataException("Attempt to remove value " + removals[removalIndex] + ", but got " + buffer[i] );
                    } 
                    entries.Addition(buffer[i]);

                    // PERF: Check if we have free space, in order to avoid copying the removals list in case
                    // an addition requires an invalidation of the removals, we check if the conditions 
                    // for a buffer growth are met. 
                    if (freeSpace == 0)
                    {
                        removals = entries.Removals;
                        freeSpace = entries.FreeSpace;
                    }
                    else
                    {
                        freeSpace--;
                    }
                }
            }

            entries.PrepareDataForCommiting();

            if (entries.TotalAdditions == 0)
            {
                Container.Delete(llt, _postingListContainerId, containerId);
                termIdInTree = -1;
                return AddEntriesToTermResult.RemoveTermId;
            }

            
            if (TryEncodingToBuffer(entries.RawAdditions, entries.TotalAdditions, tmpBuf, out var encoded) == false)
            {

                AddNewTermToSet(out termIdInTree);
                return AddEntriesToTermResult.UpdateTermId;
            }
            
            if (encoded.Length == item.Length)
            {
                var mutableSpace = Container.GetMutable(llt, containerId);
                encoded.CopyTo(mutableSpace);

                // can update in place
                termIdInTree = -1;
                return AddEntriesToTermResult.NothingToDo;
            }

            Container.Delete(llt, _postingListContainerId, containerId);
         
            termIdInTree = AllocatedSpaceForSmallSet(encoded,llt, out Span<byte> space);

            encoded.CopyTo(space);

            return AddEntriesToTermResult.UpdateTermId;
        }

        private long AllocatedSpaceForSmallSet(Span<byte> encoded, LowLevelTransaction llt, out Span<byte> space)
        {
            long termIdInTree = Container.Allocate(llt, _postingListContainerId, encoded.Length, out space);
            return EntryIdEncodings.Encode(termIdInTree, 0, TermIdMask.SmallPostingList);
        }

        private AddEntriesToTermResult AddEntriesToTermResultSingleValue(Span<byte> tmpBuf, long idInTree, ref EntriesModifications entries, out long termId)
        {
            entries.AssertPreparationIsNotFinished();
            
            var (existingEntryId, existingFrequency) = EntryIdEncodings.Decode(idInTree);
            
            // In case when existingEntryId and only addition is the same:
            // Let's assert whether the current document will output the same ID as the previous one.
            // We can assume that removals are "agnostic" for us since the already stored document has the same ID as this one.
            // In any other case, where did the different ID come from?
            if (entries.TotalAdditions == 1 && entries.Additions[0] == existingEntryId) 
            {
                if (entries.TotalRemovals > 0)
                    Debug.Assert(entries.Removals[0] == existingEntryId);
                
                var newId = EntryIdEncodings.Encode(entries.Additions[0], entries.AdditionsFrequency[0], (long)TermIdMask.Single);
                if (newId == idInTree)
                {
                    termId = -1;
                    return AddEntriesToTermResult.NothingToDo;
                }
            }
            
            if (entries.TotalAdditions == 0 && entries.TotalRemovals > 0) 
            {
                if (entries.TotalRemovals > 1) 
                    throw new InvalidOperationException($"More than one removal found for a single item, which is impossible. " +
                                                        $"{Environment.NewLine}Current tree id: {idInTree}" +
                                                        $"{Environment.NewLine}Current entry id {existingEntryId}" +
                                                        $"{Environment.NewLine}Current term frequency: {existingFrequency}" +
                                                        $"{Environment.NewLine}Items we wanted to delete (entryId|Frequency): " +
                                                        $"{string.Join(", ", entries.Removals.ToArray().Zip(entries.RemovalsFrequency.ToArray()).Select(i => $"({i.First}|{i.Second})"))}");
                
                Debug.Assert(EntryIdEncodings.QuantizeAndDequantize(entries.RemovalsFrequency[0]) == existingFrequency, "The item stored and the item we're trying to delete are different, which is impossible.");
                
                termId = -1;
                return AddEntriesToTermResult.RemoveTermId;
            }

            // Another document contains the same term. Let's check if the currently indexed document is in EntriesModification.
            // If it's not, we have to add it (since it has to be included in Small/Set too).
            if (entries.TotalAdditions >= 1)
            {
                bool isIncluded = false;
                for (int idX = 0; idX < entries.TotalAdditions && isIncluded == false; ++idX)
                {
                    if (entries.Additions[idX] == existingEntryId)
                        isIncluded = true;
                }
                
                //User may wants to delete it.
                for (int idX = 0; idX < entries.TotalRemovals && isIncluded == false; ++idX)
                {
                    if (entries.Removals[idX] == existingEntryId)
                        isIncluded = true;
                }

                if (isIncluded == false)
                    entries.Addition(existingEntryId, existingFrequency);
            }
            
            
            AddNewTerm(ref entries, tmpBuf, out termId);
            return AddEntriesToTermResult.UpdateTermId;
        }

        private AddEntriesToTermResult AddEntriesToTermResultViaLargePostingList(ref EntriesModifications entries, out long termId, long id)
        {
            var containerId = EntryIdEncodings.GetContainerId(id);
            var llt = _transaction.LowLevelTransaction;
            var setSpace = Container.GetMutable(llt, containerId);
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);
            
            entries.PrepareDataForCommiting();
            var numberOfEntries = PostingList.Update(_transaction.LowLevelTransaction, ref postingListState, entries.Additions, entries.Removals);

            termId = -1;

            if (numberOfEntries == 0)
            {
                llt.FreePage(postingListState.RootPage);
                Container.Delete(llt, _postingListContainerId, containerId);
                return AddEntriesToTermResult.RemoveTermId;
            }

            return AddEntriesToTermResult.NothingToDo;
        }

        private void InsertNumericFieldLongs(Tree fieldsTree, Tree entriesToTermsTree, IndexedField indexedField, Span<byte> tmpBuf)
        {
            var fieldTree = fieldsTree.LookupFor<Int64LookupKey>(indexedField.NameLong);
            var entriesToTerms = entriesToTermsTree.LookupFor<Int64LookupKey>(indexedField.NameLong); 

            _entriesAlreadyAdded.Clear();
            
            foreach (var (term, entries) in indexedField.Longs)
            {
                // We are not going to be using these entries anymore after this. 
                // Therefore, we can copy and we dont need to get a reference to the entry in the dictionary.
                // IMPORTANT: No modification to the dictionary can happen from this point onwards. 
                var localEntry = entries;
                if (localEntry.HasChanges() == false)
                    continue;
                  
                UpdateEntriesForTerm(entries, entriesToTerms, term);
                
                long termId;
                var hasTerm = fieldTree.TryGetValue(term, out var existing);
                if (localEntry.TotalAdditions > 0 && hasTerm == false)
                {
                    Debug.Assert(localEntry.TotalRemovals == 0, "entries.TotalRemovals == 0");
                    AddNewTerm(ref localEntry, tmpBuf, out termId);
                    
                    fieldTree.Add(term, termId);
                    continue;
                }

                switch (AddEntriesToTerm(tmpBuf, existing, ref localEntry, out termId))
                {
                    case AddEntriesToTermResult.UpdateTermId:
                        fieldTree.Add(term, termId);
                        break;
                    case AddEntriesToTermResult.RemoveTermId:
                        fieldTree.TryRemove(term);
                        break;
                }
            }
        }

        private void UpdateEntriesForTerm(EntriesModifications entries, Lookup<Int64LookupKey> entriesToTerms, long term)
        {
            SetRange(_additionsForTerm, entries.Additions);
            SetRange(_removalsForTerm, entries.Removals);

            InsertEntriesForTerm(entriesToTerms, term);
        }

        private void InsertEntriesForTerm(Lookup<Int64LookupKey> entriesToTerms, long term) 
        {
            foreach (long removal in _removalsForTerm)
            {
                // if already added, we don't need to remove it in this batch
                if (_entriesAlreadyAdded.Contains(removal))
                    continue;
                entriesToTerms.TryRemove(removal);
            }

            foreach (long addition in _additionsForTerm)
            {
                if (_entriesAlreadyAdded.Add(addition) == false)
                    continue;
                entriesToTerms.Add(addition, term);
            }
        }

        private unsafe void InsertNumericFieldDoubles(Tree fieldsTree, Tree entriesToTermsTree, IndexedField indexedField, Span<byte> tmpBuf)
        {
            var fieldTree = fieldsTree.LookupFor<Int64LookupKey>(indexedField.NameDouble);
            var entriesToTerms = entriesToTermsTree.LookupFor<Int64LookupKey>(indexedField.NameDouble); 

            _entriesAlreadyAdded.Clear();
            foreach (var (term, entries) in indexedField.Doubles)
            {
                // We are not going to be using these entries anymore after this. 
                // Therefore, we can copy and we dont need to get a reference to the entry in the dictionary.
                // IMPORTANT: No modification to the dictionary can happen from this point onwards. 
                var localEntry = entries;
                if (localEntry.HasChanges() == false)
                    continue;

                long termAsLong = BitConverter.DoubleToInt64Bits(term);
                UpdateEntriesForTerm(entries, entriesToTerms, termAsLong);
                
                var hasTerm = fieldTree.TryGetValue(termAsLong, out var existing);

                long termId;
                if (localEntry.TotalAdditions > 0 && hasTerm == false) // no existing value
                {
                    Debug.Assert(localEntry.TotalRemovals == 0, "entries.TotalRemovals == 0");
                    AddNewTerm(ref localEntry, tmpBuf, out termId);
                    fieldTree.Add(termAsLong, termId);
                    continue;
                }

                switch (AddEntriesToTerm(tmpBuf, existing, ref localEntry, out termId))
                {
                    case AddEntriesToTermResult.UpdateTermId:
                        fieldTree.Add(termAsLong, termId);
                        break;
                    case AddEntriesToTermResult.RemoveTermId:
                        fieldTree.TryRemove(termAsLong);
                        break;
                }
            }
        }
        
        private unsafe bool TryEncodingToBuffer(long * additions, int additionsCount, Span<byte> tmpBuf, out Span<byte> encoded)
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

        private unsafe void AddNewTerm(ref EntriesModifications entries, Span<byte> tmpBuf, out long termId)
        {
           
            _numberOfTermModifications += 1;
            Debug.Assert(entries.TotalAdditions > 0, "entries.TotalAdditions > 0");
            // common for unique values (guid, date, etc)
            if (entries.TotalAdditions == 1)
            {
                entries.AssertPreparationIsNotFinished();
                termId = EntryIdEncodings.Encode(*entries.RawAdditions, entries.AdditionsFrequency[0], (long)TermIdMask.Single);                
                return;
            }

            entries.PrepareDataForCommiting();
            if (TryEncodingToBuffer(entries.RawAdditions, entries.TotalAdditions, tmpBuf, out var encoded) == false)
            {
                // too big, convert to a set
                AddNewTermToSet(out termId);
                return;
            }

            termId = AllocatedSpaceForSmallSet(encoded,  _transaction.LowLevelTransaction, out Span<byte> space);
            encoded.CopyTo(space);
        }


        private unsafe void AddNewTermToSet(out long termId)
        {
            long setId = Container.Allocate(_transaction.LowLevelTransaction, _postingListContainerId, sizeof(PostingListState), out var setSpace);
            
            // we need to account for the size of the posting lists, once a term has been switch to a posting list
            // it will always be in this model, so we don't need to do any cleanup
            _largePostingListSet ??= _transaction.OpenPostingList(Constants.IndexWriter.LargePostingListsSetSlice);
            _largePostingListSet.Add(setId); 

            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);

            PostingList.Create(_transaction.LowLevelTransaction, ref postingListState, _pForEncoder);
            termId = EntryIdEncodings.Encode(setId, 0, TermIdMask.PostingList);
        }

        private void UnlikelyGrowAnalyzerBuffer(int newBufferSize, int newTokenSize)
        {
            if (newBufferSize > _encodingBufferHandler.Length)
            {
                Analyzer.BufferPool.Return(_encodingBufferHandler);
                _encodingBufferHandler = null;
                _encodingBufferHandler = Analyzer.BufferPool.Rent(newBufferSize);
            }

            if (newTokenSize > _tokensBufferHandler.Length)
            {
                Analyzer.TokensPool.Return(_tokensBufferHandler);
                _tokensBufferHandler = null;
                _tokensBufferHandler = Analyzer.TokensPool.Rent(newTokenSize);
            }
        }

        [DoesNotReturn]
        private void ThrowInvalidTokenFoundOnBuffer(IndexedField field, ReadOnlySpan<byte> value, Span<byte> wordsBuffer, Span<Token> tokens, Token token)
        {
            throw new InvalidDataException(
                $"{Environment.NewLine}Got token with: " +
                $"{Environment.NewLine}\tOFFSET {token.Offset}" +
                $"{Environment.NewLine}\tLENGTH: {token.Length}." +
                $"{Environment.NewLine}Total amount of tokens: {tokens.Length}" +
                $"{Environment.NewLine}Buffer contains '{Encodings.Utf8.GetString(wordsBuffer)}' and total length is {wordsBuffer.Length}" +
                $"{Environment.NewLine}Buffer from ArrayPool: {Environment.NewLine}\tbyte buffer is {_encodingBufferHandler.Length} {Environment.NewLine}\ttokens buffer is {_tokensBufferHandler.Length}" +
                $"{Environment.NewLine}Original span contains '{Encodings.Utf8.GetString(value)}' with total length {value.Length}" +
                $"{Environment.NewLine}Field " +
                $"{Environment.NewLine}\tid: {field.Id}" +
                $"{Environment.NewLine}\tname: {field.Name}");
        }
        
        public void Dispose()
        {
            _entriesAllocator.Dispose();
            _jsonOperationContext?.Dispose();
            if (_ownsTransaction)
                _transaction?.Dispose();
            
            if (_encodingBufferHandler != null)
            {
                Analyzer.BufferPool.Return(_encodingBufferHandler);
                _encodingBufferHandler = null;
            }
                
            if (_tokensBufferHandler != null)
            {
                Analyzer.TokensPool.Return(_tokensBufferHandler);
                _tokensBufferHandler = null;
            }

            if (_utf8ConverterBufferHandler != null)
            {
                Analyzer.BufferPool.Return(_utf8ConverterBufferHandler);
                _utf8ConverterBufferHandler = null;
            }
            
            _indexDebugDumper.Dispose();
            _builder.Clean();
        }
    }
}
