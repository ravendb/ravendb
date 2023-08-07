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
using Sparrow.Collections;
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

    public unsafe partial class IndexWriter : IDisposable // single threaded, controlled by caller
    {
        private long _numberOfModifications;
        private readonly HashSet<Slice> _indexedEntries = new(SliceComparer.Instance);
        private List<(long EntryId, float Boost)> _boostedDocs;
        private readonly IndexFieldsMapping _fieldsMapping;
        private FixedSizeTree _documentBoost;
        private Tree _indexMetadata;
        private Tree _persistedDynamicFieldsAnalyzers;
        private long _numberOfTermModifications;
        private CompactKeyCacheScope _compactKeyScope;

        private bool _ownsTransaction;
        private JsonOperationContext _jsonOperationContext;
        private readonly Transaction _transaction;

        private Token[] _tokensBufferHandler;
        private byte[] _encodingBufferHandler;
        private byte[] _utf8ConverterBufferHandler;

        internal struct TermInEntryModification : IEquatable<TermInEntryModification>, IComparable<TermInEntryModification>
        {
            public long EntryId;
            public short Frequency;

            public override string ToString() => EntryId + ", " + Frequency;

            public bool Equals(TermInEntryModification other)
            {
                return EntryId == other.EntryId && Frequency == other.Frequency;
            }

            public int CompareTo(TermInEntryModification other)
            {
                var entryIdComparison = EntryId.CompareTo(other.EntryId);
                if (entryIdComparison != 0) return entryIdComparison;
                return Frequency.CompareTo(other.Frequency);
            }
        }

        [StructLayout(LayoutKind.Sequential)]
        internal struct EntriesModifications
        {
            private const int ControlBits = 3;
            private const int ControlMask = 0b111;

            private const int HasLong = 0b001;
            private const int HasDouble = 0b010;

            private const int NeedSorting = 0b100;

            private int _termSize;
            private long _longValue;
            private double _doubleValue;

            public int TermSize
            {
                get => _termSize >> ControlBits;
                set => _termSize = _termSize & ControlMask | value << ControlBits;
            }

            public long? Long
            {
                get
                {
                    if ((_termSize & HasLong) != HasLong)
                        return null;
                    return _longValue;
                }
                set
                {
                    if (value.HasValue == false)
                    {
                        _termSize &= ~HasLong;
                    }
                    else
                    {
                        _termSize |= HasLong;
                        _longValue = value.Value;
                    }
                }
            }

            public double? Double
            {
                get
                {
                    if ((_termSize & HasDouble) != HasDouble)
                        return null;
                    return _doubleValue;
                }
                set
                {
                    if (value.HasValue == false)
                    {
                        _termSize &= ~HasDouble;
                    }
                    else
                    {
                        _termSize |= HasDouble;
                        _doubleValue = value.Value;
                    }
                }
            }


            public NativeList<TermInEntryModification> Additions;
            public NativeList<TermInEntryModification> Removals;
            public NativeList<TermInEntryModification> Updates;

            private bool NeedToSortToDeleteDuplicates
            {
                get => (_termSize & NeedSorting) != 0;
                set
                {
                    if (value == false)
                    {
                        _termSize &= ~NeedSorting;
                    }
                    else
                    {
                        _termSize |= NeedSorting;
                    }
                } 
            }
            

#if DEBUG
            private int _hasChangesCallCount = 0;
            private bool _preparationFinished = false;
#endif

            public void Prepare([NotNull] ByteStringContext context)
            {
                AssertHasChangesIsCalledOnlyOnce();

                DeleteAllDuplicates(context);
            }

            public bool HasChanges => Additions.Count + Removals.Count > 0;

            public EntriesModifications([NotNull] ByteStringContext context, int size)
            {
                TermSize = size;
                NeedToSortToDeleteDuplicates = true;
                Additions = new();
                Additions.Initialize(context);
                Removals = new();
                Removals.Initialize(context);
                Updates = new();
                Updates.Initialize(context);
            }

            public void Addition([NotNull] ByteStringContext context, long entryId, short freq = 1)
            {
                if (Additions.HasCapacityFor(1) == false)
                    Additions.Grow(context, 1);

                AddToList(ref Additions, entryId, freq);
            }
            
            public void Removal([NotNull] ByteStringContext context, long entryId, short freq = 1)
            {
                if (Removals.HasCapacityFor(1) == false)
                    Removals.Grow(context, 1);

                AddToList(ref Removals, entryId, freq);
            }

            private void AddToList(ref NativeList<TermInEntryModification> list, long entryId, short freq )
            {
                AssertPreparationIsNotFinished();
                NeedToSortToDeleteDuplicates = true;

                if (list.Count > 0)
                {
                    ref var cur = ref list.RawItems[list.Count - 1];
                    if (cur.EntryId == entryId)
                    {
                        if (cur.Frequency + freq < short.MaxValue)
                        {
                            cur.Frequency += freq;
                        }
                        else
                        {
                            cur.Frequency = short.MaxValue;
                        }

                        return;
                    }
                }

                var term = new TermInEntryModification { EntryId = entryId, Frequency = freq };
                list.PushUnsafe(term);
            }

            private void DeleteAllDuplicates([NotNull] ByteStringContext context)
            {
                if (NeedToSortToDeleteDuplicates == false)
                    return;
                NeedToSortToDeleteDuplicates = false;
                
                Additions.Sort();
                Removals.Sort();

                var oldUpdates = Updates.Count;
                int additionPos = 0, removalPos = 0;
                var additions = Additions.RawItems;
                var removals = Removals.RawItems;
                int add = 0, rem = 0;
                for (; add < Additions.Count && rem < Removals.Count; ++add)
                {
                    ref var currentAdd = ref additions[add];
                    ref var currentRemoval = ref removals[rem];

                    //We've to delete exactly same item in additions and removals and delete those.
                    //This is made for Set structure.
                    if (currentAdd.EntryId == currentRemoval.EntryId && currentAdd.Frequency == currentRemoval.Frequency)
                    {
                        if (Updates.TryPush(currentAdd) == false)
                        {
                            Updates.Grow(context, 1);
                            Updates.PushUnsafe(currentAdd);
                        }
                        rem++;
                        continue;
                    }

                    // if it is equal, then we have same entry, different freq, so need to remove & add
                    // the remove is the old one in this case
                    if (currentAdd.EntryId >= currentRemoval.EntryId)
                    {
                        removals[removalPos++] = currentRemoval;
                        rem++;
                        add--; // so the loop increment will stay the same
                        continue;
                    }
                    
                    additions[additionPos++] = currentAdd;
                }

                for (; add < Additions.Count; add++)
                {
                    additions[additionPos++] = additions[add];
                }
                for (; rem < Removals.Count; rem++)
                {
                    removals[removalPos++] = removals[rem];
                }
                Additions.Shrink(additionPos);
                Removals.Shrink(removalPos);

                Debug.Assert(oldUpdates == Updates.Count || oldUpdates == 0 && Updates.Count > 0,
                    "New updates on *second* call here should not be possible");
                
                ValidateNoDuplicateEntries();
            }

            // There is an case we found in RavenDB-19688
            // Sometimes term can be added and removed for the same in the same batch and there can be multiple other docs between this two operations.
            // This requires us to ensure we don't have duplicates here.
            public void GetEncodedAdditionsAndRemovals([NotNull] ByteStringContext context, out Span<long> additions, out Span<long> removals)
            {
                GetEncodedAdditionsAndRemovals(context, out long* addPtr, out long* removePtr);
                additions = new Span<long>(addPtr, Additions.Count);
                removals = new Span<long>(removePtr, Removals.Count);
            }

            public void GetEncodedAdditions([NotNull] ByteStringContext context, out long* additions) => GetEncodedAdditionsAndRemovals(context, out additions, out _);
            public void GetEncodedAdditionsAndRemovals([NotNull] ByteStringContext context, out long* additions, out long* removals)
            {
#if DEBUG
                if (_preparationFinished)
                    throw new InvalidOperationException(
                        $"{nameof(GetEncodedAdditionsAndRemovals)} should be called only once. This is a bug. It was called via: {Environment.NewLine}" +
                        Environment.StackTrace);
                _preparationFinished = true;
#endif
                DeleteAllDuplicates(context);

                // repurposing the memory
                Debug.Assert(sizeof(TermInEntryModification) >= sizeof(long));
                additions = (long*)Additions.RawItems;
                for (int i = 0; i < Additions.Count; i++)
                {
                    ref var cur = ref Additions.RawItems[i];
                    additions[i] = EntryIdEncodings.Encode(cur.EntryId, cur.Frequency, TermIdMask.Single);
                }

                removals = (long*)Removals.RawItems;
                for (int i = 0; i < Removals.Count; i++)
                {
                    ref var cur = ref Removals.RawItems[i];
                    removals[i] = EntryIdEncodings.Encode(cur.EntryId, cur.Frequency, TermIdMask.Single);
                }
            }

            [Conditional("DEBUG")]
            private void ValidateNoDuplicateEntries()
            {
                var removals = Removals;
                var additions = Additions;
                foreach (var add in additions.ToSpan())
                {
                    if (removals.ToSpan().IndexOf(add) >= 0)
                        throw new InvalidOperationException("Found duplicate addition & removal item during indexing: " + add);
                }

                foreach (var removal in removals.ToSpan())
                {
                    if (additions.ToSpan().IndexOf(removal) >= 0)
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
                    throw new InvalidOperationException($"{nameof(Prepare)} should be called only once.");
#endif
            }

            public void Dispose([NotNull] ByteStringContext context)
            {
                Additions.Dispose(context);
                Removals.Dispose(context);
                Updates.Dispose(context);
            }
        }

        private sealed class IndexedField
        {
            public Dictionary<long, List<(double, double)>> Spatial;
            public readonly FastList<EntriesModifications> Storage;
            public readonly Dictionary<Slice, int> Textual;
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
            public readonly bool HasSuggestions;
            public readonly bool ShouldStore;
            public bool HasMultipleTermsPerField;

            public override string ToString()
            {
                return Name.ToString()  + " Id: " + Id; 
            }

            public IndexedField(IndexFieldBinding binding) : this(binding.FieldId, binding.FieldName, binding.FieldNameLong, binding.FieldNameDouble,
                binding.FieldTermTotalSumField, binding.Analyzer, binding.FieldIndexingMode, binding.HasSuggestions, binding.ShouldStore, binding.FieldNameForStatistics)
            {
            }

            public IndexedField(int id, Slice name, Slice nameLong, Slice nameDouble, Slice nameTotalLengthOfTerms, Analyzer analyzer,
                FieldIndexingMode fieldIndexingMode, bool hasSuggestions, bool shouldStore, string nameForStatistics = null)
            {
                Name = name;
                NameLong = nameLong;
                NameDouble = nameDouble;
                NameTotalLengthOfTerms = nameTotalLengthOfTerms;
                Id = id;
                Analyzer = analyzer;
                HasSuggestions = hasSuggestions;
                ShouldStore = shouldStore;
                Storage = new FastList<EntriesModifications>();
                Textual = new Dictionary<Slice, int>(SliceComparer.Instance);
                Longs = new Dictionary<long, int>();
                Doubles = new Dictionary<double, int>();
                FieldIndexingMode = fieldIndexingMode;
                NameForStatistics = nameForStatistics ?? $"Field_{Name}";
            }

            public void Clear()
            {
                Suggestions?.Clear();
                Doubles?.Clear();
                Spatial?.Clear();
                Longs?.Clear();
                Textual?.Clear();
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
            _compactKeyScope = new(_transaction.LowLevelTransaction);
            _postingListContainerId = _transaction.OpenContainer(Constants.IndexWriter.PostingListsSlice);
            _storedFieldsContainerId = _transaction.OpenContainer(Constants.IndexWriter.StoredFieldsSlice);
            _entriesTermsContainerId = _transaction.OpenContainer(Constants.IndexWriter.EntriesTermsContainerSlice);
            _entryIdToLocation = _transaction.LookupFor<Int64LookupKey>(Constants.IndexWriter.EntryIdToLocationSlice);
            _jsonOperationContext = JsonOperationContext.ShortTermSingleUse();
            _fieldsTree = _transaction.CreateTree(Constants.IndexWriter.FieldsSlice);
            _primaryKeyTree = _fieldsTree.CompactTreeFor(_fieldsMapping.GetByFieldId(0).FieldName);
            _persistedDynamicFieldsAnalyzers = _transaction.CreateTree(Constants.IndexWriter.DynamicFieldsAnalyzersSlice);

            _indexMetadata = _transaction.CreateTree(Constants.IndexMetadataSlice);
            _initialNumberOfEntries = _indexMetadata?.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0;
            _lastEntryId =  _indexMetadata?.ReadInt64(Constants.IndexWriter.LastEntryIdSlice) ?? 0;

            _documentBoost = _transaction.FixedTreeFor(Constants.DocumentBoostSlice, sizeof(float));
            _entriesAllocator = new ByteStringContext(SharedMultipleUseFlag.None);

            _pforDecoder = new FastPForDecoder(_entriesAllocator);
        }
        public IndexWriter([NotNull] Transaction tx, IndexFieldsMapping fieldsMapping) : this(fieldsMapping)
        {
            _transaction = tx;

            _ownsTransaction = false;
            Init();
        }

        public interface IIndexEntryBuilder
        {
            void Boost(float boost);
            void WriteNull(int fieldId, string path);
            void Write(int fieldId, ReadOnlySpan<byte> value);
            void Write(int fieldId, string path, ReadOnlySpan<byte> value);
            void Write(int fieldId, string path, string value);
            void Write(int fieldId, ReadOnlySpan<byte> value, long longValue, double dblValue);
            void Write(int fieldId, string path, string value, long longValue, double dblValue);
            void Write(int fieldId, string path, ReadOnlySpan<byte> value, long longValue, double dblValue);
            void WriteSpatial(int fieldId, string path, CoraxSpatialPointEntry entry);
            void Store(BlittableJsonReaderObject storedValue);
            void Store(int fieldId, string name, BlittableJsonReaderObject storedValue);
            void RegisterEmptyOrNull(int fieldId, string fieldName, StoredFieldType type);
            void IncrementList();
            void DecrementList();
            int ResetList();
            void RestoreList(int old);
        }

        public sealed class IndexEntryBuilder : IDisposable, IIndexEntryBuilder
        {
            private readonly IndexWriter _parent;
            private long _entryId;
            public bool Active;
            private int _buildingList;
            private Tree _fieldsTree;

            public long EntryId => _entryId;

            public IndexEntryBuilder(IndexWriter parent)
            {
                _parent = parent;
            }

            public void Boost(float boost)
            {
                _parent.BoostEntry(_entryId, boost);
            }

            public void Init(long entryId)
            {
                Active = true;
                _entryId = entryId;
            }

            public void Dispose()
            {
                Active = false;
            }

            public void WriteNull(int fieldId, string path)
            {
                var field = GetField(fieldId, path);
                if (field.ShouldStore)
                {
                    RegisterEmptyOrNull(field.Name, StoredFieldType.Null);    
                }
                ExactInsert(field, Constants.NullValueSlice);
                
            }

            private IndexedField GetField(int fieldId, string path)
            {
                var field = fieldId != Constants.IndexWriter.DynamicField
                    ? _parent._knownFieldsTerms[fieldId]
                    : _parent.GetDynamicIndexedField(_parent._entriesAllocator, path);
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

                if (tokens.Length > 1)
                {
                    field.HasMultipleTermsPerField = true;
                }

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
                ByteStringContext<ByteStringMemoryCache>.InternalScope? scope = CreateNormalizedTerm(_parent._entriesAllocator, value, out var slice);

                // We are gonna try to get the reference if it exists, but we wont try to do the addition here, because to store in the
                // dictionary we need to close the slice as we are disposing it afterwards. 
                ref var termLocation = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Textual, slice, out var exists);
                if (exists == false)
                {
                    termLocation = field.Storage.Count;
                    field.Storage.AddByRef(new EntriesModifications(_parent._entriesAllocator, value.Length));
                    scope = null; // We don't want the fieldname (slice) to be returned.
                }

                if (_buildingList > 0)
                {
                    field.HasMultipleTermsPerField = true;
                }

                ref var term = ref field.Storage.GetAsRef(termLocation);
                term.Addition(_parent._entriesAllocator, _entryId);

                if (field.HasSuggestions)
                    _parent.AddSuggestions(field, slice);

                scope?.Dispose();
                
                return ref term;
            }
            
            void NumericInsert(IndexedField field, long lVal, double dVal)
            {
                // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
                ref var doublesTermsLocation = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Doubles, dVal, out bool fieldDoublesExist);
                if (fieldDoublesExist == false)
                {
                    doublesTermsLocation = field.Storage.Count;
                    field.Storage.AddByRef(new EntriesModifications(_parent._entriesAllocator, sizeof(double)));
                }

                // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
                ref var longsTermsLocation = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Longs, lVal, out bool fieldLongExist);
                if (fieldLongExist == false)
                {
                    longsTermsLocation = field.Storage.Count;
                    field.Storage.AddByRef(new EntriesModifications(_parent._entriesAllocator, sizeof(long)));
                }

                ref var doublesTerm = ref field.Storage.GetAsRef(doublesTermsLocation);
                doublesTerm.Addition(_parent._entriesAllocator, _entryId);

                ref var longsTerm = ref field.Storage.GetAsRef(longsTermsLocation);
                longsTerm.Addition(_parent._entriesAllocator, _entryId);
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
            }

            public void Write(int fieldId, ReadOnlySpan<byte> value) => Write(fieldId, null, value);

            public void Write(int fieldId, string path, ReadOnlySpan<byte> value)
            {
                var field = GetField(fieldId, path);
                if (value.Length > 0)
                {
                    if (field.ShouldStore)
                    {
                        RegisterTerm(field.Name, value, StoredFieldType.Term);
                    }
                    Insert(field, value);
                }
                else
                {
                    if (field.ShouldStore)
                    {
                        RegisterEmptyOrNull(field.Name, StoredFieldType.Empty);
                    }
                    ExactInsert(field, Constants.EmptyStringSlice);
                }
            }

            public void Write(int fieldId, string path, string value)
            {
                using var _ = Slice.From(_parent._entriesAllocator, value, out var slice);
                Write(fieldId, path, slice);
            }

            public void Write(int fieldId, ReadOnlySpan<byte> value, long longValue, double dblValue) => Write(fieldId, null, value, longValue, dblValue);
            public void Write(int fieldId, string path, string value, long longValue, double dblValue)
            {
                using var _ = Slice.From(_parent._entriesAllocator, value, out var slice);
                Write(fieldId, path, slice, longValue, dblValue);
            }

            public void Write(int fieldId, string path, ReadOnlySpan<byte> value, long longValue, double dblValue)
            {
                var field = GetField(fieldId, path);

                if (field.ShouldStore)
                {
                    RegisterTerm(field.Name, value, StoredFieldType.Tuple | StoredFieldType.Term);
                }
                
                ref var term = ref ExactInsert(field, value);
                term.Long = longValue;
                term.Double = dblValue;
                NumericInsert(field, longValue, dblValue);
                
            }

            public void WriteSpatial(int fieldId, string path, CoraxSpatialPointEntry entry)
            {
                var field = GetField(fieldId, path);
                RecordSpatialPointForEntry(field, (entry.Latitude, entry.Longitude));

                var maxLen = Encoding.UTF8.GetMaxByteCount(entry.Geohash.Length);
                using var _ = _parent._entriesAllocator.Allocate(maxLen, out var buffer);
                var len = Encoding.UTF8.GetBytes(entry.Geohash, buffer.ToSpan());
                for (int i = 1; i <= entry.Geohash.Length; ++i)
                {
                    ExactInsert(field, buffer.ToReadOnlySpan()[..i]);
                }
            }

            public void Store(BlittableJsonReaderObject storedValue)
            {
                var field = _parent._knownFieldsTerms[^1];
                if (storedValue.HasParent)
                {
                    storedValue = storedValue.CloneOnTheSameContext();
                }
                RegisterTerm(field.Name, storedValue.AsSpan(), StoredFieldType.Raw);
            }

            public void Store(int fieldId, string name, BlittableJsonReaderObject storedValue)
            {
                var field = GetField(fieldId, name);
                if (storedValue.HasParent)
                {
                    storedValue = storedValue.CloneOnTheSameContext();
                }
                RegisterTerm(field.Name, storedValue.AsSpan(), StoredFieldType.Raw);
            }


            void RegisterTerm(Slice fieldName, ReadOnlySpan<byte> term, StoredFieldType type)
            {
                if (_buildingList > 0)
                {
                    type |= StoredFieldType.List;
                }
                ref var entryTerms = ref _parent.GetEntryTerms(_entryId);

                _fieldsTree ??= _parent._transaction.CreateTree(Constants.IndexWriter.FieldsSlice);
                long fieldRootPage = _parent._fieldsCache.GetFieldRootPage(fieldName, _fieldsTree);

                var termId = Container.Allocate(
                                    _parent._transaction.LowLevelTransaction, 
                                    _parent._storedFieldsContainerId,
                                    term.Length, fieldRootPage, 
                                    out Span<byte> space);
                term.CopyTo(space);

                var recordedTerm = new RecordedTerm
                (
                    // why: entryTerms.Count << 8 
                    // we put entries count here because we are sorting the entries afterward
                    // this ensure that stored values are then read using the same order we have for writing them
                    // which is important for storing arrays
                    termContainerId: entryTerms.Count << 8 | (int)type | 0b110, // marker for stored field
                    @long: termId
                );

                if (entryTerms.TryPush(recordedTerm) == false)
                {
                    entryTerms.Grow(_parent._entriesAllocator, 1);
                    entryTerms.PushUnsafe(recordedTerm);
                }
            }

            public void RegisterEmptyOrNull(int fieldId, string fieldName, StoredFieldType type)
            {
                var field = GetField(fieldId, fieldName);
                RegisterEmptyOrNull(field.Name, type);
            }
            
            void RegisterEmptyOrNull(Slice fieldName,StoredFieldType type)
            {
                ref var entryTerms = ref _parent.GetEntryTerms(_entryId);

                _fieldsTree ??= _parent._transaction.CreateTree(Constants.IndexWriter.FieldsSlice);
                long fieldRootPage = _parent._fieldsCache.GetFieldRootPage(fieldName, _fieldsTree);

                var recordedTerm = new RecordedTerm
                (
                    // why: entryTerms.Count << 8 
                    // we put entries count here because we are sorting the entries afterward
                    // this ensure that stored values are then read using the same order we have for writing them
                    // which is important for storing arrays
                    termContainerId: entryTerms.Count << 8 | (int)type | 0b110, // marker for stored field
                    @long: fieldRootPage
                );

                if (entryTerms.TryPush(recordedTerm) == false)
                {
                    entryTerms.Grow(_parent._entriesAllocator, 1);
                    entryTerms.PushUnsafe(recordedTerm);
                }
            }

            public void IncrementList()
            {
                _buildingList++;
            }

            public void DecrementList()
            {
                _buildingList--;
            }

            public int ResetList()
            {
                var old = _buildingList;
                _buildingList = 0;
                return old;
            }

            public void RestoreList(int old)
            {
                _buildingList = old;
            }
        }

        private readonly IndexEntryBuilder _builder;

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

            _builder.Init(entryId);
            return _builder;
        }

        public IndexEntryBuilder Index(string key) => Index(Encoding.UTF8.GetBytes(key));
        
        public IndexEntryBuilder Index(ReadOnlySpan<byte> key)
        {
            long entryId = InitBuilder();

            // We do not dispose because we will be storing the slice in the hash set.
            Slice.From(_transaction.Allocator, key, ByteStringType.Immutable, out var keySlice);
            _indexedEntries.Add(keySlice);  // Register entry by key. 

            _builder.Init(entryId);

            return _builder;
        }

        private long InitBuilder()
        {
            if (_builder.Active)
                throw new NotSupportedException("You *must* dispose the previous builder before calling it again");

            _numberOfModifications++;
            var entryId = ++_lastEntryId;

            return entryId;
        }

        //Document Boost should add priority to some documents but also should not be the main component of boosting.
        //The natural logarithm slows down our scoring increase for a document so that the ranking calculated at query time is not forgotten.
        //We've to add entry container id (without frequency etc) here because in 'SortingMatch' we have already decoded ids.
        private void BoostEntry(long entryId, float documentBoost)
        {
            if (documentBoost.AlmostEquals(1f))
            {
                // We don't store `1` but if user update boost value to 1
                // we've to delete the previous one, we don't need to do this explicitly
                // since we'll delete it during ProcessDeletes()
                return;
            }

            // probably user want this to be at the same end.
            if (documentBoost <= 0f)
                documentBoost = 0;

            documentBoost = MathF.Log(documentBoost + 1); // ensure we've positive number
            _boostedDocs ??= new();
            _boostedDocs.Add((entryId, documentBoost));
        }


        /// <param name="entryId">Container id of entry (without encodings)</param>
        private void RemoveDocumentBoost(long entryId)
        {
            _documentBoost.Delete(entryId);
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
        private FastPForDecoder _pforDecoder;
        private long _lastEntryId;
        private FastPForEncoder _pForEncoder;
        private readonly Dictionary<long, NativeList<RecordedTerm>> _termsPerEntryId = new();
        private ByteStringContext _entriesAllocator;
        private Tree _fieldsTree;
        private CompactTree _primaryKeyTree;

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

            public RecordedTerm(long termContainerId)
            {
                TermContainerId = termContainerId;
                Unsafe.SkipInit(out Long);
                Unsafe.SkipInit(out Double);
            }

            public RecordedTerm(long termContainerId, long @long)
            {
                TermContainerId = termContainerId;
                Long = @long;
            }

            public RecordedTerm(long termContainerId, double lat, double lng)
            {
                TermContainerId = termContainerId;
                Lat = lat;
                Lng = lng;
            }
        }
        
        public long GetNumberOfEntries() => _initialNumberOfEntries + _numberOfModifications;

        private void AddSuggestions(IndexedField field, Slice slice)
        {
            _hasSuggestions = true;
            field.Suggestions ??= new Dictionary<Slice, int>();
            
            var keys = SuggestionsKeys.Generate(_entriesAllocator, Constants.Suggestions.DefaultNGramSize, slice.AsSpan(), out int keysCount);
            int keySizes = keys.Length / keysCount;

            var suggestionsToAdd = field.Suggestions;

            int idx = 0;
            while (idx < keysCount)
            {
                var key = new Slice(_entriesAllocator.Slice(keys, idx * keySizes, keySizes, ByteStringType.Immutable));
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


            var keys = SuggestionsKeys.Generate(_entriesAllocator, Constants.Suggestions.DefaultNGramSize, sequence, out int keysCount);
            int keySizes = keys.Length / keysCount;

            var suggestionsToAdd = field.Suggestions;

            int idx = 0;
            while (idx < keysCount)
            {
                var key = new Slice(_entriesAllocator.Slice(keys, idx * keySizes, keySizes, ByteStringType.Immutable));
                if (suggestionsToAdd.TryGetValue(key, out int counter) == false)
                    counter = 0;

                counter--;
                suggestionsToAdd[key] = counter;
                idx++;
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

        private void ProcessDeletes()
        {
            if (_deletedEntries.Count == 0)
                return;
            
            var llt = _transaction.LowLevelTransaction;
            Page lastVisitedPage = default;

            var fieldsByRootPage = GetIndexedFieldByRootPage(_fieldsTree);
            
            long dicId = CompactTree.GetDictionaryId(llt);
            var compactKey = llt.AcquireCompactKey();
            
            foreach (long entryToDelete in _deletedEntries)
            {
                if (_entryIdToLocation.TryRemove(entryToDelete, out var entryTermsId) == false)
                    throw new InvalidOperationException("Unable to locate entry id: " + entryToDelete);

                RemoveDocumentBoost(entryToDelete);
                var entryTerms = Container.MaybeGetFromSamePage(llt, ref lastVisitedPage, entryTermsId);
                RecordTermDeletionsForEntry(entryTerms, llt, fieldsByRootPage, compactKey, dicId, entryToDelete);
                Container.Delete(llt, _entriesTermsContainerId, entryTermsId);
            }
        }

        private void RecordTermDeletionsForEntry(Container.Item entryTerms, LowLevelTransaction llt, Dictionary<long, IndexedField> fieldsByRootPage, CompactKey compactKey, long dicId,
            long entryToDelete)
        {
            var reader = new EntryTermsReader(llt, entryTerms.Address, entryTerms.Length, dicId);
            reader.Reset();
            while (reader.MoveNextStoredField())
            {
                //Null/empty is not stored in container, just exists as marker.
                 if (reader.TermId == -1)
                     continue;
                
                Container.Delete(llt, _storedFieldsContainerId, reader.TermId);
            }
            reader.Reset();
            while (reader.MoveNext())
            {
                if (fieldsByRootPage.TryGetValue(reader.FieldRootPage, out var field) == false)
                {
                    throw new InvalidOperationException($"Unable to find matching field for {reader.FieldRootPage} with root page:  {reader.FieldRootPage}. Term: '{reader.Current}'");
                }
                
                var decodedKey = reader.Current.Decoded();
                var scope = Slice.From(_entriesAllocator, decodedKey, out Slice termSlice);
                if(field.HasSuggestions)
                    RemoveSuggestions(field, decodedKey);
                
                ref var termLocation = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Textual, termSlice, out var exists);
                if (exists == false)
                {
                    termLocation = field.Storage.Count;
                    field.Storage.AddByRef(new EntriesModifications(_entriesAllocator, decodedKey.Length));
                    scope = default; // We dont want to reclaim the term name
                }

                ref var term = ref field.Storage.GetAsRef(termLocation);
                term.Removal(_entriesAllocator, entryToDelete, reader.Frequency);
                scope.Dispose();
                
                if(reader.HasNumeric == false)
                    continue;

                termLocation = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Longs, reader.CurrentLong, out exists);
                if (exists == false)
                {
                    termLocation = field.Storage.Count;
                    field.Storage.AddByRef(new EntriesModifications(_entriesAllocator, sizeof(long)));
                }

                term = ref field.Storage.GetAsRef(termLocation);
                term.Removal(_entriesAllocator, entryToDelete);

                termLocation = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Doubles, reader.CurrentDouble, out exists);
                if (exists == false)
                {
                    termLocation = field.Storage.Count;
                    field.Storage.AddByRef(new EntriesModifications(_entriesAllocator, sizeof(long)));
                }

                term = ref field.Storage.GetAsRef(termLocation);
                term.Removal(_entriesAllocator, entryToDelete);
            }
        }


        public unsafe Dictionary<long, string> GetIndexedFieldNamesByRootPage()
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
                            var dynamicIndexedField = GetDynamicIndexedField(_entriesAllocator, it.CurrentKey.AsSpan());
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

        public bool TryDeleteEntry(string term)
        {
            using var _ = Slice.From(_transaction.Allocator, term, ByteStringType.Immutable, out var termSlice);
            return TryDeleteEntry(termSlice);
        }
        
        public bool TryDeleteEntry(ReadOnlySpan<byte> term)
        {
            using var __ = Slice.From(_transaction.Allocator, term, ByteStringType.Immutable, out var termSlice);
            return TryDeleteEntry(termSlice, out _);
        }
        
        private bool TryDeleteEntry(ReadOnlySpan<byte> term, out long entryId)
        {
            using var __ = Slice.From(_transaction.Allocator, term, ByteStringType.Immutable, out var termSlice);
            return TryDeleteEntry(termSlice, out entryId);
        }

        private bool TryDeleteEntry(Slice termSlice, out long entryId)
        {
            if (_indexedEntries.Contains(termSlice) == false)
            {
                _compactKeyScope.Key.Set(termSlice);
                var exists = _primaryKeyTree.TryGetValue(_compactKeyScope.Key, out var containerId);
                if (exists)
                {
                    // note that the containerId may be a single value or many(!), if it is many items
                    // we'll delete them, but treat this as a _new_ entry, not an update to an existing
                    // one
                    return RecordDeletion(containerId, out entryId);
                }
                entryId = -1;
                return false;
            }

            FlushBatch();
            return TryDeleteEntry(termSlice, out entryId);

            void FlushBatch()
            {
                // calling ResetWriter will reset the _entriesAllocator, so we need to clone it to the side
                var cloned = _transaction.Allocator.Clone(termSlice.Content);

                // We cannot actually handles modifications to the same entry in the same batch, so we cheat
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
                    _transaction.Allocator.Release(ref cloned);
                    _ownsTransaction = prevValue;
                }
            }
        }

        private void ResetWriter()
        {
            _pforDecoder.Dispose();
            _indexedEntries.Clear();
            _termsPerEntryId.Clear();
            _deletedEntries.Clear();
            _entriesAlreadyAdded.Clear();
            _additionsForTerm.Clear();
            _removalsForTerm.Clear();

            for (int i = 0; i < _knownFieldsTerms.Length; i++)
            {
                _knownFieldsTerms[i].Clear();
            }
            if (_dynamicFieldsTerms != null)
            {
                foreach (var (_, field)  in _dynamicFieldsTerms)
                {
                    field.Clear();
                }
            }
            
            _entriesAllocator.Reset();
            
            _pforDecoder = new FastPForDecoder(_entriesAllocator);
        }

        public void TryDeleteEntryByField(string field, string term)
        {
            using var __ = Slice.From(_entriesAllocator, term, ByteStringType.Immutable, out var termSlice);
            using var ___ = Slice.From(_entriesAllocator, field, ByteStringType.Immutable, out var fieldSlice);
            if (TryGetEntryTermId(fieldSlice, termSlice.AsSpan(), out long idInTree) == false) 
                return;

            RecordDeletion(idInTree, out _);
        }
  
        
        /// <summary>
        /// Record term for deletion from Index.
        /// </summary>
        /// <param name="idInTree">With frequencies and container type.</param>
        [SkipLocalsInit]
        private bool RecordDeletion(long idInTree, out long singleEntryId)
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

                singleEntryId = -1;
                return false;
                
            }

            if ((idInTree & (long)TermIdMask.SmallPostingList) != 0)
            {
                var smallSet = Container.Get(_transaction.LowLevelTransaction, containerId);
                // combine with existing value
                _ = VariableSizeEncoding.Read<int>(smallSet.Address, out var pos);
                
                _pforDecoder.Init(smallSet.Address + pos, smallSet.Length - pos);
                var output = stackalloc long[1024];
                while (true)
                {
                    var read = _pforDecoder.Read(output, 1024);
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
                singleEntryId = -1;
                return false;
            }

            singleEntryId = containerId;
            _deletedEntries.Add(containerId);
            _numberOfModifications--;
            return true;
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

        public void Commit() => Commit<EmptyStatsScope>(default);
        
        public void Commit<TStatsScope>(TStatsScope stats)
            where TStatsScope : struct, ICoraxStatsScope
        {
            _indexDebugDumper.Commit();
            using var _ = _entriesAllocator.Allocate(Container.MaxSizeInsideContainerPage, out Span<byte> workingBuffer);

            using (stats.For(CommitOperation.Deletions))
                ProcessDeletes();
            
            Tree entriesToTermsTree = _transaction.CreateTree(Constants.IndexWriter.EntriesToTermsSlice);
            Tree entriesToSpatialTree = _transaction.CreateTree(Constants.IndexWriter.EntriesToSpatialSlice);
            _indexMetadata.Increment(Constants.IndexWriter.NumberOfEntriesSlice, _numberOfModifications);
            _indexMetadata.Increment(Constants.IndexWriter.NumberOfTermsInIndex, _numberOfTermModifications);
            _indexMetadata.Add(Constants.IndexWriter.LastEntryIdSlice, _lastEntryId);
            _pForEncoder = new FastPForEncoder(_entriesAllocator);

            if (_boostedDocs != null)
                AppendDocumentsBoost();
            
            Slice[] keys = Array.Empty<Slice>();
            for (int fieldId = 0; fieldId < _fieldsMapping.Count; ++fieldId)
            {
                var indexedField = _knownFieldsTerms[fieldId];
                using var staticFieldScope = stats.For(indexedField.NameForStatistics);

                if (indexedField.Textual.Count == 0)
                    continue;

                using (staticFieldScope.For(CommitOperation.TextualValues))
                    InsertTextualField(entriesToTermsTree, indexedField, workingBuffer, ref keys);
                
                using (staticFieldScope.For(CommitOperation.IntegerValues))
                    InsertNumericFieldLongs(entriesToTermsTree, indexedField, workingBuffer);
                
                using (staticFieldScope.For(CommitOperation.FloatingValues))
                    InsertNumericFieldDoubles(entriesToTermsTree, indexedField, workingBuffer);

                using (staticFieldScope.For(CommitOperation.SpatialValues))
                    InsertSpatialField(entriesToSpatialTree, indexedField);

                if (indexedField.HasMultipleTermsPerField)
                {
                    _indexMetadata.MultiAdd(Constants.IndexWriter.MultipleTermsInField, indexedField.Name);
                }
            }

            if (_dynamicFieldsTerms != null)
            {
                foreach (var (_, indexedField) in _dynamicFieldsTerms)
                {
                    using var dynamicFieldScope = stats.For(indexedField.NameForStatistics);

                    using (dynamicFieldScope.For(CommitOperation.TextualValues))
                        InsertTextualField(entriesToTermsTree, indexedField, workingBuffer, ref keys);

                    using (dynamicFieldScope.For(CommitOperation.IntegerValues))
                        InsertNumericFieldLongs(entriesToTermsTree, indexedField, workingBuffer);
                    
                    using (dynamicFieldScope.For(CommitOperation.FloatingValues))
                        InsertNumericFieldDoubles(entriesToTermsTree, indexedField, workingBuffer);
                    
                    using (dynamicFieldScope.For(CommitOperation.SpatialValues))
                        InsertSpatialField(entriesToSpatialTree,  indexedField);
                    
                    if (indexedField.HasMultipleTermsPerField)
                    {
                        _indexMetadata.MultiAdd(Constants.IndexWriter.MultipleTermsInField, indexedField.Name);
                    }
                }
            }

            using(stats.For(CommitOperation.StoredValues))
                WriteIndexEntries();

            _pForEncoder.Dispose();
            _pForEncoder = null;
            
            if(keys.Length>0)
                ArrayPool<Slice>.Shared.Return(keys);

            // Check if we have suggestions to deal with. 
            if (_hasSuggestions)
            {
                using var __ = stats.For(CommitOperation.Suggestions);
                for (var fieldId = 0; fieldId < _knownFieldsTerms.Length; fieldId++)
                {
                    IndexedField indexedField = _knownFieldsTerms[fieldId];
                    if (indexedField.Suggestions == null) continue;
                    Slice.From(_entriesAllocator, $"{SuggestionsTreePrefix}{fieldId}", out var treeName);
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

        private void AppendDocumentsBoost()
        {
            _boostedDocs.Sort();
            foreach (var (entryId, documentBoost) in _boostedDocs)
            {
                using var __ = _documentBoost.DirectAdd(entryId, out var isNew, out byte* boostPtr);
                float* floatBoostPtr = (float*)boostPtr;
                *floatBoostPtr = documentBoost;
            }
        }

        private void WriteIndexEntries()
        {
            using var writer = new EntryTermsWriter(_entriesAllocator);
            foreach (var (entry, termsRef) in _termsPerEntryId)
            {
                int size = writer.Encode(termsRef);
                long entryTermsId = Container.Allocate(_transaction.LowLevelTransaction, _entriesTermsContainerId, size, out var space);
                writer.Write(space);
                _entryIdToLocation.Add(entry, entryTermsId);
            }
        }

        private void InsertSpatialField(Tree entriesToSpatialTree, IndexedField indexedField)
        {
            if (indexedField.Spatial == null)
                return;

            var fieldRootPage = _fieldsTree.GetLookupRootPage(indexedField.Name);
            Debug.Assert(fieldRootPage != -1);
            var termContainerId = fieldRootPage << 3 | 0b010;
            Debug.Assert(termContainerId >>> 3 == fieldRootPage, "field root too high?");
            var entriesToTerms = entriesToSpatialTree.FixedTreeFor(indexedField.Name, sizeof(double)+sizeof(double));
            foreach (var (entry, list)  in indexedField.Spatial)
            {
                list.Sort();

                ref var entryTerms = ref GetEntryTerms(entry);
                foreach (var item in CollectionsMarshal.AsSpan(list))
                {
                    var (lat,lng) = item;
                    var recordedTerm = new RecordedTerm
                    (
                        termContainerId: termContainerId,
                        lat: lat,
                        lng: lng
                    );

                    if (entryTerms.TryPush(recordedTerm) == false)
                    {
                        entryTerms.Grow(_entriesAllocator, 1);
                        entryTerms.PushUnsafe(recordedTerm);
                    }
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

        private void InsertTextualField(Tree entriesToTermsTree, IndexedField indexedField, Span<byte> tmpBuf, ref Slice[] sortedTermsBuffer)
        {
            var fieldTree = _fieldsTree.CompactTreeFor(indexedField.Name);
            
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

            using var dumper = new IndexTermDumper(_fieldsTree, indexedField.Name);

            fieldTree.InitializeStateForTryGetNextValue();
            long totalLengthOfTerm = 0;

            using var compactKeyCacheScope = new CompactKeyCacheScope(_transaction.LowLevelTransaction);

            var entriesForTerm = new NativeList<TermInEntryModification>();
            entriesForTerm.Initialize(_entriesAllocator);

            for (var index = 0; index < termsCount; index++)
            {
                var term = sortedTermsBuffer[index];
                ref var entriesLocation = ref CollectionsMarshal.GetValueRefOrNullRef(currentFieldTerms, term);
                Debug.Assert(Unsafe.IsNullRef(ref entriesLocation) == false);
                
                ref var entries = ref indexedField.Storage.GetAsRef(entriesLocation);
                entries.Prepare(_entriesAllocator);
                
                UpdateEntriesForTerm(ref entriesForTerm, entries);

                if (indexedField.Spatial == null) // For spatial, we handle this in InsertSpatialField, so we skip it here
                {
                    SetRange(_additionsForTerm, entries.Additions);
                    SetRange(_removalsForTerm, entries.Removals);
                }

                compactKeyCacheScope.Key.Set(term.AsSpan());
                bool found = fieldTree.TryGetNextValue(compactKeyCacheScope.Key, out var termContainerId, out var existingIdInTree, out var keyLookup);
                Debug.Assert(found || entries.Removals.Count == 0, "Cannot remove entries from term that isn't already there");

                if (entries.HasChanges)
                {
                    long termId;
                    if (entries.Additions.Count > 0 && found == false)
                    {
                        if (entries.Removals.Count != 0)
                            throw new InvalidOperationException($"Attempt to remove entries from new term: '{term}' for field {indexedField.Name}! This is a bug.");

                        AddNewTerm(ref entries, tmpBuf, out termId);
                        totalLengthOfTerm += entries.TermSize;

                        dumper.WriteAddition(term, termId);
                        termContainerId = fieldTree.AddAfterTryGetNext(ref keyLookup, termId);
                    }
                    else
                    {
                        var entriesToTermResult = AddEntriesToTerm(tmpBuf, existingIdInTree, ref entries, out termId);
                        switch (entriesToTermResult)
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
                                if (fieldTree.TryRemoveExistingValue(ref keyLookup, out var oldValue) == false)
                                {
                                    dumper.WriteRemoval(term, termId);
                                    ThrowTriedToDeleteTermThatDoesNotExists();
                                }

                                totalLengthOfTerm -= entries.TermSize;
                                dumper.WriteRemoval(term, oldValue);
                                _numberOfTermModifications--;
                                break;
                            case AddEntriesToTermResult.NothingToDo:
                                break;
                            default:
                                throw new ArgumentOutOfRangeException(entriesToTermResult.ToString());
                        }

                        void ThrowTriedToDeleteTermThatDoesNotExists()
                        {
                            throw new InvalidOperationException(
                                $"Attempt to remove term: '{term}' for field {indexedField.Name}, but it does not exists! This is a bug.");
                        }
                    }
                }

                RecordTermsForEntries(entriesForTerm, entries, termContainerId);
                
                if (indexedField.Spatial == null)
                {
                    Debug.Assert(termContainerId > 0);
                    InsertEntriesForTerm(entriesToTerms, termContainerId);
                }
            }

            entriesForTerm.Dispose(_entriesAllocator);

            _indexMetadata.Increment(indexedField.NameTotalLengthOfTerms, totalLengthOfTerm);
        }

        private void RecordTermsForEntries(in NativeList<TermInEntryModification> entriesForTerm, in EntriesModifications entries, long termContainerId)
        {
            var entriesForTermCount = entriesForTerm.Count;
            var rawItems = entriesForTerm.RawItems;
            
            for (int i = 0; i < entriesForTermCount; i++)
            {
                var entry = rawItems[i];

                ref var recordedTermList = ref GetEntryTerms(entry.EntryId);

                if ( recordedTermList.HasCapacityFor(1) == false)
                    recordedTermList.Grow(_entriesAllocator, 1);

                ref var recordedTerm = ref recordedTermList.AddByRefUnsafe();

                Debug.Assert((termContainerId & 0b111) == 0); // ensure that the three bottom bits are cleared

                long recordedTermContainerId = entry.Frequency switch
                {
                    > 1 => termContainerId << 8 | // note, bottom 3 are cleared, so we have 11 bits to play with
                           EntryIdEncodings.FrequencyQuantization(entry.Frequency) << 3 |
                           0b100, // marker indicating that we have a term frequency here
                    _ => termContainerId
                };

                if (entries.Long != null)
                {
                    recordedTermContainerId |= 1; // marker!
                    recordedTerm.Long = entries.Long.Value;

                    // only if the double value can not be computed by casting from long, we store it 
                    if (entries.Double != null && entries.Double.Value != recordedTerm.Long)
                    {
                        recordedTermContainerId |= 2; // marker!
                        recordedTerm.Double = entries.Double.Value;
                    }
                }

                recordedTerm.TermContainerId = recordedTermContainerId;
            }
        }

        private void UpdateEntriesForTerm(ref NativeList<TermInEntryModification> entriesForTerm, in EntriesModifications entries)
        {
            entriesForTerm.ResetAndEnsureCapacity(_entriesAllocator, entries.Additions.Count + entries.Updates.Count);

            int i = 0;
            var additions = entries.Additions.RawItems;
            for (; i < entries.Additions.Count; i++)
            {
                entriesForTerm.PushUnsafe(additions[i]);
            }

            var updates = entries.Updates.RawItems;
            for (; i < entries.Updates.Count; i++)
            {
                entriesForTerm.PushUnsafe(updates[i]);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ref NativeList<RecordedTerm> GetEntryTerms(long entry)
        {
            ref var entryTerms = ref CollectionsMarshal.GetValueRefOrAddDefault(_termsPerEntryId, entry, out var exists);
            if (exists == false)
            {
                entryTerms = new NativeList<RecordedTerm>();
                entryTerms.Initialize(_entriesAllocator);
            }
            return ref entryTerms;
        }

        private void SetRange(List<long> list, in NativeList<TermInEntryModification> span)
        {
            list.Clear();
            for (int i = 0; i < span.Count; i++)
            {
                list.Add(span.RawItems[i].EntryId);
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

        [SkipLocalsInit]
        private AddEntriesToTermResult AddEntriesToTermResultViaSmallPostingList(Span<byte> tmpBuf, ref EntriesModifications entries, out long termIdInTree, long idInTree)
        {
            var containerId = EntryIdEncodings.GetContainerId(idInTree);
            
            var llt = _transaction.LowLevelTransaction;
            var item = Container.Get(llt, containerId);
            Debug.Assert(entries.Removals.ToSpan().ToArray().Distinct().Count() == entries.Removals.Count, $"Removals list is not distinct.");

            
            // combine with existing values

            // PERF: We use SkipLocalsInit because we don't need to ensure this stack space to be filled with zeroes
            // which diminish the amount of work this method has to do.
            var buffer = stackalloc long[1024];
           
            _ = VariableSizeEncoding.Read<int>(item.Address, out var offset); // discard count here
            _pforDecoder.Init(item.Address + offset, item.Length - offset);

            int removalIndex = 0;
            var removals = entries.Removals;
            var bufferAsSpan = new Span<long>(buffer, 1024);
            while (true)
            {
                var read = _pforDecoder.Read(buffer, 1024);
                if (read == 0)
                    break;

                EntryIdEncodings.DecodeAndDiscardFrequency(bufferAsSpan, read);
                for (int i = 0; i < read; i++)
                {
                    if (removalIndex < removals.Count)
                    {
                        long entryId = removals.RawItems[removalIndex].EntryId;
                        if (buffer[i] == entryId)
                        {
                            removalIndex++;
                            continue;
                        }

                        if (buffer[i] > entryId)
                            throw new InvalidDataException("Attempt to remove value " + entryId + ", but got " + buffer[i] );
                    } 
                    entries.Addition(_entriesAllocator, buffer[i]);
                }
            }

            entries.GetEncodedAdditions(_entriesAllocator, out var additions);

            if (entries.Additions.Count == 0)
            {
                Container.Delete(llt, _postingListContainerId, containerId);
                termIdInTree = -1;
                return AddEntriesToTermResult.RemoveTermId;
            }

            
            if (TryEncodingToBuffer(additions, entries.Additions.Count, tmpBuf, out var encoded) == false)
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
            var additions = entries.Additions.RawItems;
            if (entries.Additions.Count == 1) 
            {
                ref var single = ref additions[0];
                if (single.EntryId == existingEntryId)
                {
                    Debug.Assert(entries.Removals.Count == 0 || entries.Removals.RawItems[0].EntryId == existingEntryId);

                    var newId = EntryIdEncodings.Encode(single.EntryId, single.Frequency, (long)TermIdMask.Single);
                    if (newId == idInTree)
                    {
                        termId = -1;
                        return AddEntriesToTermResult.NothingToDo;
                    }
                }
            }
            
            if (entries.Additions.Count == 0 && entries.Removals.Count > 0) 
            {
                if (entries.Removals.Count > 1)
                {
                    throw new InvalidOperationException($"More than one removal found for a single item, which is impossible. " +
                                                        $"{Environment.NewLine}Current tree id: {idInTree}" +
                                                        $"{Environment.NewLine}Current entry id {existingEntryId}" +
                                                        $"{Environment.NewLine}Current term frequency: {existingFrequency}" +
                                                        $"{Environment.NewLine}Items we wanted to delete (entryId|Frequency): " +
                                                        $"{string.Join(", ", entries.Removals.ToSpan().ToArray().Select(i => $"({i.EntryId}|{i.Frequency})"))}");
                }
                
                Debug.Assert(EntryIdEncodings.QuantizeAndDequantize(entries.Removals.RawItems[0].Frequency) == existingFrequency, "The item stored and the item we're trying to delete are different, which is impossible.");
                
                termId = -1;
                return AddEntriesToTermResult.RemoveTermId;
            }

            // Another document contains the same term. Let's check if the currently indexed document is in EntriesModification.
            // If it's not, we have to add it (since it has to be included in Small/Set too).
            if (entries.Additions.Count >= 1)
            {
                bool isIncluded = false;
                for (int idX = 0; idX < entries.Additions.Count && isIncluded == false; ++idX)
                {
                    if (entries.Additions.RawItems[idX].EntryId == existingEntryId)
                        isIncluded = true;
                }
                
                //User may wants to delete it.
                for (int idX = 0; idX < entries.Removals.Count && isIncluded == false; ++idX)
                {
                    if (entries.Removals.RawItems[idX].EntryId == existingEntryId)
                        isIncluded = true;
                }

                if (isIncluded == false)
                    entries.Addition(_entriesAllocator, existingEntryId, existingFrequency);
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
            
            entries.GetEncodedAdditionsAndRemovals(_entriesAllocator, out Span<long> additions, out Span<long> removals);
            var numberOfEntries = PostingList.Update(_transaction.LowLevelTransaction, ref postingListState, additions, removals);

            termId = -1;

            if (numberOfEntries == 0)
            {
                llt.FreePage(postingListState.RootPage);
                
                Container.Delete(llt, _postingListContainerId, containerId);
                RemovePostingListFromLargePostingListsSet(containerId);
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

        private void InsertNumericFieldLongs(Tree entriesToTermsTree, IndexedField indexedField, Span<byte> tmpBuf)
        {
            var fieldTree = _fieldsTree.LookupFor<Int64LookupKey>(indexedField.NameLong);
            var entriesToTerms = entriesToTermsTree.LookupFor<Int64LookupKey>(indexedField.NameLong); 

            _entriesAlreadyAdded.Clear();
            
            foreach (var (term, entriesLocation) in indexedField.Longs)
            {
                ref var entries = ref indexedField.Storage.GetAsRef(entriesLocation);

                // We are not going to be using these entries anymore after this. 
                // Therefore, we can copy and we dont need to get a reference to the entry in the dictionary.
                // IMPORTANT: No modification to the dictionary can happen from this point onwards. 
                var localEntry = entries;
                localEntry.Prepare(_entriesAllocator);
                if (localEntry.HasChanges == false)
                    continue;
                  
                UpdateEntriesForTerm(entries, entriesToTerms, term);
                
                long termId;
                var hasTerm = fieldTree.TryGetValue(term, out var existing);
                if (localEntry.Additions.Count > 0 && hasTerm == false)
                {
                    Debug.Assert(localEntry.Removals.Count == 0, "entries.TotalRemovals == 0");
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

        private unsafe void InsertNumericFieldDoubles(Tree entriesToTermsTree, IndexedField indexedField, Span<byte> tmpBuf)
        {
            var fieldTree = _fieldsTree.LookupFor<DoubleLookupKey>(indexedField.NameDouble);
            var entriesToTerms = entriesToTermsTree.LookupFor<Int64LookupKey>(indexedField.NameDouble); 

            _entriesAlreadyAdded.Clear();
            foreach (var (term, entriesLocation) in indexedField.Doubles)
            {
                ref var entries = ref indexedField.Storage.GetAsRef(entriesLocation);

                // We are not going to be using these entries anymore after this. 
                // Therefore, we can copy and we dont need to get a reference to the entry in the dictionary.
                // IMPORTANT: No modification to the dictionary can happen from this point onwards. 
                var localEntry = entries;
                localEntry.Prepare(_entriesAllocator);
                
                if (localEntry.HasChanges == false)
                    continue;

                UpdateEntriesForTerm(entries, entriesToTerms,  BitConverter.DoubleToInt64Bits(term));
                
                var hasTerm = fieldTree.TryGetValue(term, out var existing);

                long termId;
                if (localEntry.Additions.Count > 0 && hasTerm == false) // no existing value
                {
                    Debug.Assert(localEntry.Removals.Count == 0, "entries.TotalRemovals == 0");
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
        
        private bool TryEncodingToBuffer(long * additions, int additionsCount, Span<byte> tmpBuf, out Span<byte> encoded)
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

        private void AddNewTerm(ref EntriesModifications entries, Span<byte> tmpBuf, out long termId)
        {
            _numberOfTermModifications += 1;
            Debug.Assert(entries.Additions.Count > 0, "entries.TotalAdditions > 0");
            // common for unique values (guid, date, etc)
            if (entries.Additions.Count == 1)
            {
                entries.AssertPreparationIsNotFinished();
                ref var single = ref entries.Additions.RawItems[0]; 
                termId = EntryIdEncodings.Encode(single.EntryId, single.Frequency, (long)TermIdMask.Single);                
                return;
            }

            entries.GetEncodedAdditions(_entriesAllocator, out var additions);
            if (TryEncodingToBuffer(additions, entries.Additions.Count, tmpBuf, out var encoded) == false)
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
            _compactKeyScope.Dispose();
            _pforDecoder.Dispose();
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
