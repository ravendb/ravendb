using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Mappings;
using Corax.Pipeline;
using Corax.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Containers;
using Voron.Data.Fixed;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util;
using InvalidOperationException = System.InvalidOperationException;

namespace Corax
{
    // container ids are guaranteed to be aligned on 
    // 4 bytes boundary, we're using this to store metadata
    // about the data
    public enum TermIdMask : long
    {
        Single = 0,
        
        EnsureIsSingleMask = 0b11,
        
        Small = 1,
        Set = 2
    }

    public partial class IndexWriter : IDisposable // single threaded, controlled by caller
    {
        private long _numberOfModifications;
        private readonly IndexFieldsMapping _fieldsMapping;
        private FixedSizeTree _documentBoost;
        private readonly Tree _indexMetadata;
        private readonly Tree _persistedDynamicFieldsAnalyzers;
        private readonly StorageEnvironment _environment;
        private long _numberOfTermModifications;

        private readonly bool _ownsTransaction;
        private JsonOperationContext _jsonOperationContext;
        public readonly Transaction Transaction;
        private readonly TransactionPersistentContext _transactionPersistentContext;

        private Token[] _tokensBufferHandler;
        private byte[] _encodingBufferHandler;
        private byte[] _utf8ConverterBufferHandler;

        // CPU bound - embarassingly parallel
        // 
        // private readonly ConcurrentDictionary<Slice, Dictionary<Slice, ConcurrentQueue<long>>> _bufferConcurrent =
        //     new ConcurrentDictionary<Slice, ConcurrentDictionary<Slice, ConcurrentQueue<long>>>(SliceComparer.Instance);

        internal unsafe struct EntriesModifications : IDisposable
        {
            private readonly ByteStringContext _context;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _memoryHandler;

            private long* _start;
            private long* _end;
            private int _additions;
            private int _removals;
            
            public int TermSize => _sizeOfTerm;
            private readonly int _sizeOfTerm;


            private short* _freqStart;
            private short* _freqEnd;

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
                
                _start = (long*)output.Ptr;
                _end = _start + InitialSize;
                _freqStart = (short*)_end;
                _freqEnd = _freqStart + InitialSize ;
                new Span<short>(_freqStart, InitialSize).Fill(1);
                _additions = 0;
                _removals = 0;
            }

            private static int CalculateSizeOfContainer(int size) => size * (sizeof(long) + sizeof(short));
            
            public void Addition(long entryId, short freq = 1)
            {
                AssertPreparationIsNotFinished();

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
                Unsafe.CopyBlockUnaligned( freqEnd - _removals, _freqEnd - _removals, (uint)_removals * sizeof(short));
                
                //All new items are 1 by default
                new Span<short>(freqStart + _additions, newTotalSpace - (_additions+_removals)).Fill(1);


#if DEBUG
                var additionsBufferEqual = new Span<long>(_start, _additions).SequenceEqual(new Span<long>(start, _additions));
                var removalsBufferEqual =  new Span<long>(_end - _removals, _removals).SequenceEqual(new Span<long>(end - _removals, _removals));
                var additionsFrequencyBufferEqual =  new Span<short>(_freqStart, _additions).SequenceEqual(new Span<short>(freqStart, _additions));
                var removalsFrequencyBufferEqual =  new Span<short>(_freqEnd - _removals, _removals).SequenceEqual(new Span<short>(freqEnd - _removals, _removals));
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
                    throw new InvalidOperationException($"{nameof(PrepareDataForCommiting)} should be called only once. This is a bug. It was called via: {Environment.NewLine}" + Environment.StackTrace);
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
            public readonly Dictionary<Slice, EntriesModifications> Textual;
            public readonly Dictionary<long, EntriesModifications> Longs;
            public readonly Dictionary<double, EntriesModifications> Doubles;
            public Dictionary<Slice, int> Suggestions;
            public readonly Analyzer Analyzer;
            public readonly bool HasSuggestions;
            public readonly Slice Name;
            public readonly Slice NameLong;
            public readonly Slice NameDouble;
            public readonly Slice NameTotalLengthOfTerms;
            public readonly int Id;
            public readonly FieldIndexingMode FieldIndexingMode;

            public IndexedField(IndexFieldBinding binding) : this(binding.FieldId, binding.FieldName, binding.FieldNameLong, binding.FieldNameDouble, binding.FieldTermTotalSumField, binding.Analyzer, binding.FieldIndexingMode, binding.HasSuggestions)
            {
            }
            
            public IndexedField(int id, Slice name, Slice nameLong, Slice nameDouble, Slice nameTotalLengthOfTerms, Analyzer analyzer, FieldIndexingMode fieldIndexingMode, bool hasSuggestions)
            {
                Name = name;
                NameLong = nameLong;
                NameDouble = nameDouble;
                NameTotalLengthOfTerms = nameTotalLengthOfTerms;
                Id = id;
                Analyzer = analyzer;
                HasSuggestions = hasSuggestions;
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

        private readonly long _postingListContainerId, _entriesContainerId;
        private IndexFieldsMapping _dynamicFieldsMapping;

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
            _fieldsMapping = fieldsMapping;
            _encodingBufferHandler = Analyzer.BufferPool.Rent(fieldsMapping.MaximumOutputSize);
            _tokensBufferHandler = Analyzer.TokensPool.Rent(fieldsMapping.MaximumTokenSize);
            _utf8ConverterBufferHandler = Analyzer.BufferPool.Rent(fieldsMapping.MaximumOutputSize * 10);

            var bufferSize = fieldsMapping!.Count;
            _knownFieldsTerms = new IndexedField[bufferSize];
            for (int i = 0; i < bufferSize; ++i)
                _knownFieldsTerms[i] = new IndexedField(fieldsMapping.GetByFieldId(i));
        }
        
        public IndexWriter([NotNull] StorageEnvironment environment, IndexFieldsMapping fieldsMapping) : this(fieldsMapping)
        {
            _environment = environment;
            _transactionPersistentContext = new TransactionPersistentContext(true);
            Transaction = _environment.WriteTransaction(_transactionPersistentContext);

            _ownsTransaction = true;
            _postingListContainerId = Transaction.OpenContainer(Constants.IndexWriter.PostingListsSlice);
            _entriesContainerId = Transaction.OpenContainer(Constants.IndexWriter.EntriesContainerSlice);
            _jsonOperationContext = JsonOperationContext.ShortTermSingleUse();
            _indexMetadata = Transaction.CreateTree(Constants.IndexMetadataSlice);
            _documentBoost = Transaction.FixedTreeFor(Constants.DocumentBoostSlice, sizeof(float));

        }

        public IndexWriter([NotNull] Transaction tx, IndexFieldsMapping fieldsMapping, bool hasDynamics) : this(tx, fieldsMapping)
        {
            _persistedDynamicFieldsAnalyzers = Transaction.CreateTree(Constants.IndexWriter.DynamicFieldsAnalyzersSlice);
        }
        
        public IndexWriter([NotNull] Transaction tx, IndexFieldsMapping fieldsMapping) : this(fieldsMapping)
        {
            Transaction = tx;

            _ownsTransaction = false;
            _postingListContainerId = Transaction.OpenContainer(Constants.IndexWriter.PostingListsSlice);
            _entriesContainerId = Transaction.OpenContainer(Constants.IndexWriter.EntriesContainerSlice);
            _indexMetadata = Transaction.CreateTree(Constants.IndexMetadataSlice);
            _documentBoost = Transaction.FixedTreeFor(Constants.DocumentBoostSlice, sizeof(float));
        }

        public long Index(string id, Span<byte> data)
        {
            using var _ = Slice.From(Transaction.Allocator, id, out var idSlice);
            return Index(idSlice, data);
        }

        public long Index(string id, Span<byte> data, float documentBoost)
        {
            var entryId = Index(id, data);
            AppendDocumentBoost(entryId, documentBoost);
            return entryId;
        }
        
        private unsafe long Index(Slice id, Span<byte> data)
        {
            _numberOfModifications++;
            Span<byte> buf = stackalloc byte[10];
            var idLen = ZigZagEncoding.Encode(buf, id.Size);
            int requiredSize = idLen + id.Size + data.Length;
            // align to 16 bytes boundary to ensure that we have some (small) space for updating in-place entries
            requiredSize += 16 - (requiredSize % 16);
            var entryId = Container.Allocate(Transaction.LowLevelTransaction, _entriesContainerId, requiredSize, out var space);
            buf.Slice(0, idLen).CopyTo(space);
            space = space.Slice(idLen);
            id.CopyTo(space);
            space = space.Slice(id.Size);
            data.CopyTo(space);
            space = space.Slice(data.Length);
            space.Clear();// clean any old data that may have already been there

            var context = Transaction.Allocator;

            fixed (byte* newEntryDataPtr = data)
            {
                var entryReader = new IndexEntryReader(newEntryDataPtr, data.Length);

                foreach (var binding in _fieldsMapping)
                {
                    if (binding.FieldIndexingMode is FieldIndexingMode.No)
                        continue;

                    var indexer = new TermIndexer(this, context, entryReader.GetFieldReaderFor(binding.FieldId), _knownFieldsTerms[binding.FieldId], entryId);
                    indexer.InsertToken();
                }

                var it = new IndexEntryReader.DynamicFieldEnumerator(entryReader);
                while (it.MoveNext())
                {
                    var fieldReader = entryReader.GetFieldReaderFor(it.CurrentFieldName);

                    var indexedField = GetDynamicIndexedField(context, ref it);

                    if (indexedField.FieldIndexingMode is FieldIndexingMode.No)
                        continue;


                    var indexer = new TermIndexer(this, context, fieldReader, indexedField, entryId);
                    indexer.InsertToken();
                }

                return EntryIdEncodings.Encode(entryId, 1, TermIdMask.Single);
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
                    _documentBoost.Delete(EntryIdEncodings.DecodeAndDiscardFrequency(entryId));
                
                return;
            }

            // probably user want this to be at the same end.
            if (documentBoost <= 0f)
                documentBoost = 0;

            documentBoost = MathF.Log(documentBoost + 1); // ensure we've positive number
            
            using var __ = _documentBoost.DirectAdd(EntryIdEncodings.DecodeAndDiscardFrequency(entryId), out _, out byte* boostPtr);
            float* floatBoostPtr = (float*)boostPtr;
            *floatBoostPtr = documentBoost;
        }

        private unsafe void RemoveDocumentBoost(long entryId)
        {
            _documentBoost.Delete(EntryIdEncodings.DecodeAndDiscardFrequency(entryId));
        }

        public unsafe long Update(string field, Span<byte> key, LazyStringValue id, Span<byte> data, ref long numberOfEntries, float documentBoost)
        {
            var entryId = Update(field, key, id, data, ref numberOfEntries);
            AppendDocumentBoost(entryId, documentBoost, true);
            return entryId;
        }
        
       // idInTree - encoded with frequency and container type
        public long Update(string field, Span<byte> key, LazyStringValue id, Span<byte> data, ref long numberOfEntries)
        {
            if (TryGetEntryTermId(field, key, out var idInTree) == false)
            {
                numberOfEntries++;
                return Index(id, data);
            }
            // if there is more than a single entry for this key, delete & index from scratch
            // this is checked by calling code, but cheap to do this here as well.
            if((idInTree & (long)TermIdMask.EnsureIsSingleMask) != 0)
            {
                RecordDeletion(idInTree);
                numberOfEntries++;
                return Index(id, data);
            }
            
            Page lastVisitedPage = default;

            var entryId = EntryIdEncodings.DecodeAndDiscardFrequency(idInTree);
            var oldEntryReader = IndexSearcher.GetEntryReaderFor(Transaction, ref lastVisitedPage, entryId, out var rawSize);
            
            if (oldEntryReader.Buffer.SequenceEqual(data))
                return idInTree; // no change, can skip all work here, joy!
           
            Span<byte> buf = stackalloc byte[10];
            var idLen = ZigZagEncoding.Encode(buf, id.Size);

            // can't fit in old size, have to remove anyway
            if (rawSize < idLen + id.Size + data.Length)
            {
                RecordDeletion(idInTree);
                numberOfEntries++;
                return Index(id, data);
            }
            var context = Transaction.Allocator;

            // we can fit it in the old space, let's, great!
            foreach (var fieldBinding in _fieldsMapping)
            {
                if (fieldBinding.IsIndexed == false)
                    continue;

                UpdateModifiedTermsOnly(context, ref oldEntryReader, data, fieldBinding, entryId);
            }

            // now we can update the actual details here...
            var space = Container.GetMutable(Transaction.LowLevelTransaction, entryId);
            buf.Slice(0, idLen).CopyTo(space);
            space = space.Slice(idLen);
            id.AsSpan().CopyTo(space);
            space = space.Slice(id.Size);
            data.CopyTo(space);
            space = space.Slice(data.Length);
            space.Clear(); // remove any extra data from old value
          
            return idInTree;
        }

        private unsafe void UpdateModifiedTermsOnly(ByteStringContext context, ref IndexEntryReader oldEntryReader, Span<byte> newEntryData,
            IndexFieldBinding fieldBinding, long entryId)
        {
            fixed (byte* newEntryDataPtr = newEntryData)
            {
                var newEntryReader = new IndexEntryReader(newEntryDataPtr, newEntryData.Length);

                var oldType = oldEntryReader.GetFieldType(fieldBinding.FieldId, out var _);
                var newType = newEntryReader.GetFieldType(fieldBinding.FieldId, out var _);

                var indexedField = _knownFieldsTerms[fieldBinding.FieldId];
                var newFieldReader = newEntryReader.GetFieldReaderFor(fieldBinding.FieldId);
                var oldFieldReader = oldEntryReader.GetFieldReaderFor(fieldBinding.FieldId);
                if (oldType != newType)
                {
                    RemoveSingleTerm(indexedField, oldFieldReader, entryId);
                    var indexer = new TermIndexer(this, context, newFieldReader, indexedField, entryId);
                    indexer.InsertToken();
                    return;
                }

                switch (oldType)
                {
                    case IndexEntryFieldType.Empty:
                    case IndexEntryFieldType.Null:
                        // nothing _can_ change here
                        break;
                    case IndexEntryFieldType.TupleListWithNulls:
                    case IndexEntryFieldType.TupleList:
                    case IndexEntryFieldType.ListWithNulls:
                    case IndexEntryFieldType.List:
                        {
                            bool oldHasIterator = oldFieldReader.TryReadMany(out var oldIterator);
                            bool newHasIterator = newFieldReader.TryReadMany(out var newIterator);
                            bool areEqual = oldHasIterator == newHasIterator;
                            while (true)
                            {
                                oldHasIterator = oldIterator.ReadNext();
                                newHasIterator = newIterator.ReadNext();

                                if (oldHasIterator != newHasIterator)
                                {
                                    areEqual = false;
                                    break;
                                }

                                if (oldHasIterator == false)
                                    break;

                                if (oldIterator.Type != newIterator.Type)
                                {
                                    areEqual = false;
                                    break;
                                }

                                if (oldIterator.Sequence.SequenceEqual(newIterator.Sequence) == false)
                                {
                                    areEqual = false;
                                    break;
                                }
                            }

                            if (areEqual == false)
                            {
                                RemoveSingleTerm(indexedField, oldFieldReader, entryId);
                                var indexer = new TermIndexer(this, context, newFieldReader, indexedField, entryId);
                                indexer.InsertToken();
                            }
                            break;
                        }
                    case IndexEntryFieldType.Tuple:
                    case IndexEntryFieldType.SpatialPoint:
                    case IndexEntryFieldType.Simple:
                        {
                            bool hasOld = oldFieldReader.Read(out var oldVal);
                            bool hasNew = newFieldReader.Read(out var newVal);
                            if (hasOld != hasNew || hasOld && oldVal.SequenceEqual(newVal) == false)
                            {
                                RemoveSingleTerm(indexedField, oldFieldReader, entryId);
                                var indexer = new TermIndexer(this, context, newFieldReader, indexedField, entryId);
                                indexer.InsertToken();
                            }
                            break;
                        }
                    case IndexEntryFieldType.Raw:
                    case IndexEntryFieldType.RawList:
                    case IndexEntryFieldType.Invalid:
                        break;

                    case IndexEntryFieldType.SpatialPointList:
                        {
                            bool oldHasIterator = oldFieldReader.TryReadManySpatialPoint(out var oldIterator);
                            bool newHasIterator = newFieldReader.TryReadManySpatialPoint(out var newIterator);
                            bool areEqual = oldHasIterator == newHasIterator;
                            while (true)
                            {
                                oldHasIterator = oldIterator.ReadNext();
                                newHasIterator = newIterator.ReadNext();

                                if (oldHasIterator != newHasIterator)
                                {
                                    areEqual = false;
                                    break;
                                }

                                if (oldHasIterator == false)
                                    break;

                                if (oldIterator.Type != newIterator.Type)
                                {
                                    areEqual = false;
                                    break;
                                }

                                if (oldIterator.Geohash.SequenceEqual(newIterator.Geohash) == false)
                                {
                                    areEqual = false;
                                    break;
                                }
                            }

                            if (areEqual == false)
                            {
                                RemoveSingleTerm(indexedField, oldFieldReader, entryId);
                                var indexer = new TermIndexer(this, context, newFieldReader, indexedField, entryId);
                                indexer.InsertToken();
                            }
                            break;
                        }
                }
            }
        }
        
        private IndexedField GetDynamicIndexedField(ByteStringContext context, ref IndexEntryReader.DynamicFieldEnumerator it)
        {
            _dynamicFieldsTerms ??= new(SliceComparer.Instance);
            using var _ = Slice.From(context, it.CurrentFieldName, out var slice);

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
                
                indexedField = new IndexedField(Constants.IndexWriter.DynamicField, binding.FieldName, binding.FieldNameLong, binding.FieldNameDouble, binding.FieldTermTotalSumField, binding.Analyzer, binding.FieldIndexingMode, binding.HasSuggestions);
                
                if (persistedAnalyzer != null)
                {
                    var originalIndexingMode = (FieldIndexingMode)persistedAnalyzer.Reader.ReadByte();
                    if (binding.FieldIndexingMode != originalIndexingMode)
                        throw new InvalidDataException($"Inconsistent dynamic field creation options were detected. Field '{binding.FieldName}' was created with '{originalIndexingMode}' analyzer but now '{binding.FieldIndexingMode}' analyzer was specified. This is not supported");
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
                indexedField = new IndexedField(Constants.IndexWriter.DynamicField, clonedFieldName, fieldNameLong, fieldNameDouble, nameSum, analyzer, mode, false);
                _dynamicFieldsTerms[clonedFieldName] = indexedField;
            }
        }

        public long GetNumberOfEntries() => (_indexMetadata.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0) + _numberOfModifications;

        private void AddSuggestions(IndexedField field, Slice slice)
        {
            _hasSuggestions = true;
            field.Suggestions ??= new Dictionary<Slice, int>();
            
            var keys = SuggestionsKeys.Generate(Transaction.Allocator, Constants.Suggestions.DefaultNGramSize, slice.AsSpan(), out int keysCount);
            int keySizes = keys.Length / keysCount;

            var bsc = Transaction.Allocator;

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


            var keys = SuggestionsKeys.Generate(Transaction.Allocator, Constants.Suggestions.DefaultNGramSize, sequence, out int keysCount);
            int keySizes = keys.Length / keysCount;

            var bsc = Transaction.Allocator;
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
                                ExactInsert(Constants.NullValueSlice);
                                NumericInsert(0L, double.NaN);
                            }
                            else if (iterator.IsEmptyString)
                            {
                                throw new InvalidDataException("Tuple list cannot contain an empty string (otherwise, where did the numeric came from!)");
                            }
                            else
                            {
                                ExactInsert(iterator.Sequence);
                                NumericInsert(iterator.Long, iterator.Double);
                            }
                        }

                        break;

                    case IndexEntryFieldType.Tuple:
                        if (_fieldReader.Read(out _, out long lVal, out double dVal, out Span<byte> valueInEntry) == false)
                            break;

                        ExactInsert(valueInEntry);
                        NumericInsert(lVal, dVal);
                        break;

                    case IndexEntryFieldType.SpatialPointList:
                        if (_fieldReader.TryReadManySpatialPoint(out var spatialIterator) == false)
                            break;

                        while (spatialIterator.ReadNext())
                        {
                            for (int i = 1; i <= spatialIterator.Geohash.Length; ++i)
                                ExactInsert(spatialIterator.Geohash.Slice(0, i));
                        }

                        break;

                    case IndexEntryFieldType.SpatialPoint:
                        if (_fieldReader.Read(out valueInEntry) == false)
                            break;

                        for (int i = 1; i <= valueInEntry.Length; ++i)
                            ExactInsert(valueInEntry.Slice(0, i));

                        break;

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

            void NumericInsert(long lVal, double dVal)
            {
                // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
                ref var doublesTerms = ref CollectionsMarshal.GetValueRefOrAddDefault(_indexedField.Doubles, dVal, out bool fieldDoublesExist);
                if (fieldDoublesExist == false)
                    doublesTerms = new EntriesModifications(_parent.Transaction.Allocator, sizeof(double));
                doublesTerms.Addition(_entryId);

                // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
                ref var longsTerms = ref CollectionsMarshal.GetValueRefOrAddDefault(_indexedField.Longs, lVal, out bool fieldLongExist);
                if (fieldLongExist == false)
                    longsTerms = new EntriesModifications(_parent.Transaction.Allocator, sizeof(long));
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

            void ExactInsert(ReadOnlySpan<byte> value)
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

        private void RecordTermsToDeleteFrom(long entryToDelete,  LowLevelTransaction llt, ref Page lastVisitedPage)
        {
            var entryReader = IndexSearcher.GetEntryReaderFor(Transaction, ref lastVisitedPage, entryToDelete, out var _);
            foreach (var binding in _fieldsMapping)
            {
                if (binding.IsIndexed == false)
                    continue;

                RemoveSingleTerm(_knownFieldsTerms[binding.FieldId], entryReader.GetFieldReaderFor(binding.FieldId), entryToDelete);
            }

            var context = Transaction.Allocator;
            var it = new IndexEntryReader.DynamicFieldEnumerator(entryReader);
            while (it.MoveNext())
            {
                var indexedField = GetDynamicIndexedField(context, ref it);
                var fieldReader = entryReader.GetFieldReaderFor(it.CurrentFieldName);
                RemoveSingleTerm(indexedField, fieldReader, entryToDelete);
            }

            Container.Delete(llt, _entriesContainerId, entryToDelete); // delete raw index entry
        }

        private void RemoveSingleTerm(IndexedField indexedField, in IndexEntryReader.FieldReader fieldReader, long entryToDelete)
        {
            var context = Transaction.Allocator;

            switch (fieldReader.Type)
            {
                case IndexEntryFieldType.Empty:
                case IndexEntryFieldType.Null:
                    var termValue = fieldReader.Type == IndexEntryFieldType.Null ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                    RecordExactTermToDelete(termValue, indexedField);
                    break;
                case IndexEntryFieldType.TupleListWithNulls:
                case IndexEntryFieldType.TupleList:
                {
                    if (fieldReader.TryReadMany(out var iterator) == false)
                        break;

                    while (iterator.ReadNext())
                    {
                        if (iterator.IsNull)
                        {
                            RecordTupleToDelete(indexedField, Constants.NullValueSlice, double.NaN, 0);
                        }
                        else if (iterator.IsEmptyString)
                        {
                            throw new InvalidDataException("Tuple list cannot contain an empty string (otherwise, where did the numeric came from!)");
                        }
                        else
                        {
                            RecordTupleToDelete(indexedField, iterator.Sequence, iterator.Double, iterator.Long);
                        }
                    }

                    break;
                }
                case IndexEntryFieldType.Tuple:
                    if (fieldReader.Read(out _, out long l, out double d, out Span<byte> valueInEntry) == false)
                        break;
                    RecordTupleToDelete(indexedField, valueInEntry, d, l);
                    break;

                case IndexEntryFieldType.SpatialPointList:
                    if (fieldReader.TryReadManySpatialPoint(out var spatialIterator) == false)
                        break;

                    while (spatialIterator.ReadNext())
                    {
                        for (int i = 1; i <= spatialIterator.Geohash.Length; ++i)
                        {
                            var spatialTerm = spatialIterator.Geohash.Slice(0, i);
                            RecordExactTermToDelete(spatialTerm, indexedField);
                        }
                    }

                    break;
                case IndexEntryFieldType.Raw:
                case IndexEntryFieldType.RawList:
                case IndexEntryFieldType.Invalid:
                    break;
                case IndexEntryFieldType.List:
                case IndexEntryFieldType.ListWithNulls:
                {
                    if (fieldReader.TryReadMany(out var iterator) == false)
                        break;

                    while (iterator.ReadNext())
                    {
                        if (iterator.IsNull)
                        {
                            RecordExactTermToDelete(Constants.NullValueSlice, indexedField);
                        }
                        else if (iterator.IsEmptyString)
                        {
                            RecordExactTermToDelete(Constants.EmptyStringSlice, indexedField);
                        }
                        else
                        {
                            RecordTermToDelete(iterator.Sequence, indexedField);
                        }
                    }

                    break;
                }

                case IndexEntryFieldType.SpatialPoint:
                    if (fieldReader.Read(out valueInEntry) == false)
                        break;

                    for (int i = 1; i <= valueInEntry.Length; ++i)
                    {
                        var spatialTerm = valueInEntry.Slice(0, i);
                        RecordExactTermToDelete(spatialTerm, indexedField);
                    }

                    break;
                default:
                    if (fieldReader.Read(out var value) == false)
                        break;

                    if (value.IsEmpty)
                        goto case IndexEntryFieldType.Empty;

                    RecordTermToDelete(value, indexedField);
                    break;
            }

            void RecordTupleToDelete(IndexedField indexedField, ReadOnlySpan<byte> termValue, double termDouble, long termLong)
            {
                RecordExactTermToDelete(termValue, indexedField);

                // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
                ref var doublesTerms = ref CollectionsMarshal.GetValueRefOrAddDefault(indexedField.Doubles, termDouble, out bool fieldDoublesExist);
                if (fieldDoublesExist == false)
                    doublesTerms = new EntriesModifications(context, sizeof(double));
                doublesTerms.Removal(entryToDelete);

                // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
                ref var longsTerms = ref CollectionsMarshal.GetValueRefOrAddDefault(indexedField.Longs, termLong, out bool fieldLongExist);
                if (fieldLongExist == false)
                    longsTerms = new EntriesModifications(context, sizeof(long));
                longsTerms.Removal(entryToDelete);
            }

            void RecordTermToDelete(ReadOnlySpan<byte> termValue, IndexedField indexedField)
            {
                if (indexedField.HasSuggestions)
                    RemoveSuggestions(indexedField, termValue);

                var analyzer = indexedField.Analyzer;
                if (analyzer== null)
                {
                    RecordExactTermToDelete(termValue, indexedField);
                    return;
                }
                
                if (termValue.Length > _encodingBufferHandler.Length)
                {
                    analyzer.GetOutputBuffersSize(termValue.Length, out int outputSize, out int tokenSize);
                    if (outputSize > _encodingBufferHandler.Length || tokenSize > _tokensBufferHandler.Length)
                        UnlikelyGrowAnalyzerBuffer(outputSize, tokenSize);
                }

                var tokenSpace = _tokensBufferHandler.AsSpan();
                var wordSpace = _encodingBufferHandler.AsSpan();
                analyzer.Execute(termValue, ref wordSpace, ref tokenSpace, ref _utf8ConverterBufferHandler);

                for (int i = 0; i < tokenSpace.Length; i++)
                {
                    ref var token = ref tokenSpace[i];

                    var term = wordSpace.Slice(token.Offset, (int)token.Length);
                    RecordExactTermToDelete(term, indexedField);
                }
            }
            
            void RecordExactTermToDelete(ReadOnlySpan<byte> termValue, IndexedField field)
            {
                ByteStringContext<ByteStringMemoryCache>.InternalScope? scope = CreateNormalizedTerm(context, termValue, out Slice termSlice);

                // We are gonna try to get the reference if it exists, but we wont try to do the addition here, because to store in the
                // dictionary we need to close the slice as we are disposing it afterwards. 
                ref var term = ref CollectionsMarshal.GetValueRefOrAddDefault(field.Textual, termSlice, out var exists);
                if (exists == false)
                {
                    term = new EntriesModifications(context, termValue.Length);
                    scope = null; // We dont want to reclaim the term name
                }

                term.Removal(entryToDelete);

                scope?.Dispose();
            }
        }

        private void ProcessDeletes() 
        {
            var llt = Transaction.LowLevelTransaction;
            Page lastVisitedPage = default;
            foreach (long entryToDelete in _deletedEntries)
            {
                RemoveDocumentBoost(entryToDelete);
                RecordTermsToDeleteFrom(entryToDelete, llt, ref lastVisitedPage);
            }
        }

        public bool TryDeleteEntry(string key, string term)
        {
            using var _ = Slice.From(Transaction.Allocator, term, ByteStringType.Immutable, out var termSlice);

            if (TryGetEntryTermId(key, termSlice.AsSpan(), out long idInTree) == false) 
                return false;

            RecordDeletion(idInTree);
            return true;
        }
        
        public bool TryDeleteEntry(string key, string term, out long entriesCountDifference)
        {
            var originAmountOfModifications = _numberOfModifications;
            var result = TryDeleteEntry(key, term);
            entriesCountDifference = _numberOfModifications - originAmountOfModifications;
            
            return result;
        }
        
        /// <summary>
        /// Record term for deletion from Index.
        /// </summary>
        /// <param name="idInTree">With frequencies and container type.</param>
        private void RecordDeletion(long idInTree)
        {
            if ((idInTree & (long)TermIdMask.Set) != 0)
            {
                var id = EntryIdEncodings.GetContainerId(idInTree);
                var setSpace = Container.GetMutable(Transaction.LowLevelTransaction, id);
                ref var setState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);
                
                using var set = new PostingList(Transaction.LowLevelTransaction, Slices.Empty, setState);
                var iterator = set.Iterate();
                while (iterator.MoveNext())
                {
                    // since this is also encoded we've to delete frequency and container type as well
                    _deletedEntries.Add(EntryIdEncodings.DecodeAndDiscardFrequency(iterator.Current));
                    _numberOfModifications--;
                }
            }
            else if ((idInTree & (long)TermIdMask.Small) != 0)
            {
                var id = EntryIdEncodings.GetContainerId(idInTree);
                var smallSet = Container.Get(Transaction.LowLevelTransaction, id).ToSpan();
                // combine with existing value
                var cur = 0L;
                var count = ZigZagEncoding.Decode<int>(smallSet, out var pos);
                for (int idX = 0; idX < count; ++idX)
                {
                    var value = ZigZagEncoding.Decode<long>(smallSet, out var len, pos);
                    pos += len;
                    cur += value;
                    _deletedEntries.Add(EntryIdEncodings.DecodeAndDiscardFrequency(cur));
                    _numberOfModifications--;
                }
            }
            else
            {
                _deletedEntries.Add(EntryIdEncodings.DecodeAndDiscardFrequency(idInTree));
                _numberOfModifications--;
            }
        }

        /// <summary>
        /// Get TermId (id of container) from FieldTree 
        /// </summary>
        /// <param name="idInTree">Has frequency and container type inside idInTree.</param>
        /// <returns></returns>
        private bool TryGetEntryTermId(string key, Span<byte> term, out long idInTree)
        {
            var fieldsTree = Transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
            if (fieldsTree == null)
            {
                idInTree = -1;
                return false;
            }

            var fieldTree = fieldsTree.CompactTreeFor(key);

            // We need to normalize the term in case we have a term bigger than MaxTermLength.
            using var __ = CreateNormalizedTerm(Transaction.Allocator, term, out var termSlice);

            var termValue = termSlice.AsReadOnlySpan();
            return fieldTree.TryGetValue(termValue, out idInTree);
        }
        
        public void Commit()
        {
            using var _ = Transaction.Allocator.Allocate(Container.MaxSizeInsideContainerPage, out Span<byte> workingBuffer);
            Tree fieldsTree = Transaction.CreateTree(Constants.IndexWriter.FieldsSlice);
            _indexMetadata.Increment(Constants.IndexWriter.NumberOfEntriesSlice, _numberOfModifications);
            _indexMetadata.Increment(Constants.IndexWriter.NumberOfTermsInIndex, _numberOfTermModifications);
            ProcessDeletes();

            Slice[] keys = Array.Empty<Slice>();
            
            for (int fieldId = 0; fieldId < _fieldsMapping.Count; ++fieldId)
            {
                var indexedField = _knownFieldsTerms[fieldId];
                if (indexedField.Textual.Count == 0)
                    continue; 

                InsertTextualField(fieldsTree, indexedField, workingBuffer, ref keys);
                InsertNumericFieldLongs(fieldsTree, indexedField, workingBuffer);
                InsertNumericFieldDoubles(fieldsTree, indexedField, workingBuffer);
            }

            if (_dynamicFieldsTerms != null)
            {
                foreach (var (_, indexedField) in _dynamicFieldsTerms)
                {
                    InsertTextualField(fieldsTree, indexedField, workingBuffer, ref keys);
                    InsertNumericFieldLongs(fieldsTree, indexedField, workingBuffer);
                    InsertNumericFieldDoubles(fieldsTree, indexedField, workingBuffer);

                }
            }
            
            if(keys.Length>0)
                ArrayPool<Slice>.Shared.Return(keys);

            // Check if we have suggestions to deal with. 
            if (_hasSuggestions)
            {
                for (var fieldId = 0; fieldId < _knownFieldsTerms.Length; fieldId++)
                {
                    IndexedField indexedField = _knownFieldsTerms[fieldId];
                    if (indexedField.Suggestions == null) continue;
                    Slice.From(Transaction.Allocator, $"{SuggestionsTreePrefix}{fieldId}", out var treeName);
                    var tree = Transaction.CompactTreeFor(treeName);
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
                Transaction.Commit();
            }
        }


        private void InsertTextualField(Tree fieldsTree, IndexedField indexedField, Span<byte> tmpBuf, ref Slice[] sortedTermsBuffer)
        {
            var fieldTree = fieldsTree.CompactTreeFor(indexedField.Name);
            var currentFieldTerms = indexedField.Textual;
            int termsCount = currentFieldTerms.Count;

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
            for (var index = 0; index < termsCount; index++)
            {
                var term = sortedTermsBuffer[index];
                ref var entries = ref CollectionsMarshal.GetValueRefOrNullRef(currentFieldTerms, term);
                Debug.Assert(Unsafe.IsNullRef(ref entries) == false);
                if (entries.HasChanges() == false)
                    continue;
                
                long termId;
                ReadOnlySpan<byte> termsSpan = term.AsSpan();
                
                bool found = fieldTree.TryGetNextValue(termsSpan, out var existing, out var scope);
                if (entries.TotalAdditions > 0 && found == false)
                {
                    if (entries.TotalRemovals != 0)
                        throw new InvalidOperationException($"Attempt to remove entries from new term: '{term}' for field {indexedField.Name}! This is a bug.");

                    AddNewTerm(ref entries, tmpBuf, out termId);
                    _indexMetadata.Increment(indexedField.NameTotalLengthOfTerms, entries.TermSize);
                    
                    dumper.WriteAddition(term, termId);
                    fieldTree.Add(scope.Key, termId);
                }
                else
                {
                    switch (AddEntriesToTerm(tmpBuf, existing, ref entries, out termId))
                    {
                        case AddEntriesToTermResult.UpdateTermId:
                            dumper.WriteAddition(term, termId);
                            fieldTree.Add(scope.Key, termId);
                            break;
                        case AddEntriesToTermResult.RemoveTermId:
                            if (fieldTree.TryRemove(termsSpan, out var ttt) == false)
                            {
                                dumper.WriteRemoval(term, termId);
                                throw new InvalidOperationException($"Attempt to remove term: '{term}' for field {indexedField.Name}, but it does not exists! This is a bug.");
                            }

                            _indexMetadata.Increment(indexedField.NameTotalLengthOfTerms, -entries.TermSize);
                            dumper.WriteRemoval(term, ttt);
                            _numberOfTermModifications--;
                            break;
                        case AddEntriesToTermResult.NothingToDo:
                            break;
                    }
                }

                scope.Dispose();
            }
        }

        private enum AddEntriesToTermResult
        {
            NothingToDo,
            UpdateTermId,
            RemoveTermId
        }

        private AddEntriesToTermResult AddEntriesToTerm(Span<byte> tmpBuf, long existing, ref EntriesModifications entries, out long termId)
        {
            if ((existing & (long)TermIdMask.Set) != 0)
            {
                return AddEntriesToTermResultViaLargeSet(ref entries, out termId, existing & Constants.StorageMask.ContainerType);
            }
            if ((existing & (long)TermIdMask.Small) != 0)
            {
                return AddEntriesToTermResultViaSmallSet(tmpBuf, ref entries, out termId, existing & Constants.StorageMask.ContainerType);
            }
            return AddEntriesToTermResultSingleValue(tmpBuf, existing, ref entries, out termId);
        }

        private AddEntriesToTermResult AddEntriesToTermResultViaSmallSet(Span<byte> tmpBuf, ref EntriesModifications entries, out long termId, long id)
        {
            id = EntryIdEncodings.GetContainerId(id);
            
            var llt = Transaction.LowLevelTransaction;
            var smallSet = Container.GetMutable(llt, id);
            Debug.Assert(entries.Removals.ToArray().Distinct().Count() == entries.TotalRemovals, $"Removals list is not distinct.");
            int removalIndex = 0;
            
            // combine with existing values
            var currentId = 0L;
            var count = ZigZagEncoding.Decode<int>(smallSet, out var positionInEncodedBuffer);

            var removals = entries.Removals;
            long freeSpace = entries.FreeSpace;
            for (int idX = 0; idX < count; ++idX)
            {
                var value = ZigZagEncoding.Decode<long>(smallSet, out var lengthOfDelta, positionInEncodedBuffer);
                positionInEncodedBuffer += lengthOfDelta;
                currentId += value;

                var entryId = EntryIdEncodings.DecodeAndDiscardFrequency(currentId);
                if (removalIndex < removals.Length)
                {
                    if (entryId == removals[removalIndex])
                    {
                        removalIndex++;
                        continue;
                    }

                    if (entryId > removals[removalIndex])
                        throw new InvalidDataException("Attempt to remove value " + removals[removalIndex] + ", but got " + currentId);
                }

                entries.Addition(entryId);

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

            entries.PrepareDataForCommiting();

            if (entries.TotalAdditions == 0)
            {
                Container.Delete(llt, _postingListContainerId, id);
                termId = -1;
                return AddEntriesToTermResult.RemoveTermId;
            }

            if (TryDeltaEncodingToBuffer(entries.Additions, tmpBuf, out var encoded) == false)
            {
                AddNewTermToSet(entries.Additions, out termId);
                return AddEntriesToTermResult.UpdateTermId;
            }

            if (encoded.TryCopyTo(smallSet))
            {
                // can update in place
                termId = -1;
                return AddEntriesToTermResult.NothingToDo;
            }

            Container.Delete(llt, _postingListContainerId, id);
            var allocatedSize = encoded.Length + 32 - (encoded.Length % 32);

            termId = Container.Allocate(llt, _postingListContainerId, allocatedSize, out var space);
            termId = EntryIdEncodings.Encode(termId, 0, TermIdMask.Small);
            
            encoded.CopyTo(space);
            return AddEntriesToTermResult.UpdateTermId;
        }

        private AddEntriesToTermResult AddEntriesToTermResultSingleValue(Span<byte> tmpBuf, long existing, ref EntriesModifications entries, out long termId)
        {
            entries.AssertPreparationIsNotFinished();
            var (existingEntryId, existingFrequency) = EntryIdEncodings.Decode(existing);

            // single
            if (entries.TotalAdditions == 1 && entries.Additions[0] == existingEntryId && entries.AdditionsFrequency[0] == existingFrequency && entries.TotalRemovals == 0)
            {
                // Same element to add, nothing to do here.
                termId = -1;
                return AddEntriesToTermResult.NothingToDo;
            }

            if (entries.TotalRemovals != 0)
            {
                if (entries.Removals[0] != existingEntryId || entries.RemovalsFrequency[0] != existingFrequency || entries.TotalRemovals != 1)
                    throw new InvalidDataException($"Attempt to delete id {string.Join(", ", entries.Removals.ToArray())} that does not exists, only value is: {existing}");

                if (entries.TotalAdditions == 0)
                {
                    termId = -1;
                    return AddEntriesToTermResult.RemoveTermId;
                }
            }
            else
            {
                entries.Addition(existingEntryId, existingFrequency);
            }
            
            AddNewTerm(ref entries, tmpBuf, out termId);
            return AddEntriesToTermResult.UpdateTermId;
        }

        private AddEntriesToTermResult AddEntriesToTermResultViaLargeSet(ref EntriesModifications entries, out long termId, long id)
        {
            id = EntryIdEncodings.GetContainerId(id);
            var llt = Transaction.LowLevelTransaction;
            var setSpace = Container.GetMutable(llt, id);
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);
            
            entries.PrepareDataForCommiting();
            var numberOfEntries = PostingList.Update(Transaction.LowLevelTransaction, ref postingListState, entries.Additions, entries.Removals);

            termId = -1;

            if (numberOfEntries == 0)
            {
                llt.FreePage(postingListState.RootPage);
                Container.Delete(llt, _postingListContainerId, id);
                return AddEntriesToTermResult.RemoveTermId;
            }

            return AddEntriesToTermResult.NothingToDo;
        }

        private unsafe void InsertNumericFieldLongs(Tree fieldsTree, IndexedField indexedField, Span<byte> tmpBuf)
        {
            FixedSizeTree fieldTree = fieldsTree.FixedTreeFor(indexedField.NameLong, sizeof(long));
          
            foreach (var (term, entries) in indexedField.Longs)
            {
                // We are not going to be using these entries anymore after this. 
                // Therefore, we can copy and we dont need to get a reference to the entry in the dictionary.
                // IMPORTANT: No modification to the dictionary can happen from this point onwards. 
                var localEntry = entries;
                if (localEntry.HasChanges() == false)
                    continue;
                
                long termId;
                using var _ = fieldTree.Read(term, out var result);
                if (localEntry.TotalAdditions > 0 && result.HasValue == false)
                {
                    Debug.Assert(localEntry.TotalRemovals == 0, "entries.TotalRemovals == 0");
                    AddNewTerm(ref localEntry, tmpBuf, out termId);
                    
                    fieldTree.Add(term, termId);
                    continue;
                }

                var existing = *((long*)result.Content.Ptr);
                switch (AddEntriesToTerm(tmpBuf, existing, ref localEntry, out termId))
                {
                    case AddEntriesToTermResult.UpdateTermId:
                        fieldTree.Add(term, termId);
                        break;
                    case AddEntriesToTermResult.RemoveTermId:
                        fieldTree.Delete(term);
                        break;
                }
            }
        }
        
        private unsafe void InsertNumericFieldDoubles(Tree fieldsTree, IndexedField indexedField, Span<byte> tmpBuf)
        {
            var fieldTree = fieldsTree.FixedTreeForDouble(indexedField.NameDouble, sizeof(long));

            foreach (var (term, entries) in indexedField.Doubles)
            {
                // We are not going to be using these entries anymore after this. 
                // Therefore, we can copy and we dont need to get a reference to the entry in the dictionary.
                // IMPORTANT: No modification to the dictionary can happen from this point onwards. 
                var localEntry = entries;
                if (localEntry.HasChanges() == false)
                    continue;
                
                using var _ = fieldTree.Read(term, out var result);

                long termId;
                if (localEntry.TotalAdditions > 0 && result.Size == 0) // no existing value
                {
                    Debug.Assert(localEntry.TotalRemovals == 0, "entries.TotalRemovals == 0");
                    AddNewTerm(ref localEntry, tmpBuf, out termId);
                    fieldTree.Add(term, termId);
                    continue;
                }

                var existing = *((long*)result.Content.Ptr);
                switch (AddEntriesToTerm(tmpBuf, existing, ref localEntry, out termId))
                {
                    case AddEntriesToTermResult.UpdateTermId:
                        fieldTree.Add(term, termId);
                        break;
                    case AddEntriesToTermResult.RemoveTermId:
                        fieldTree.Delete(term);
                        break;
                }
            }
        }
        
        private unsafe bool TryDeltaEncodingToBuffer(ReadOnlySpan<long> additions, Span<byte> tmpBuf, out Span<byte> encoded)
        {
            // try to insert to container value
            //TODO: using simplest delta encoding, need to do better here
            fixed (byte* tmpBufferPtr = tmpBuf)
            {
                int pos = ZigZagEncoding.Encode(tmpBufferPtr, additions.Length);
                pos += ZigZagEncoding.Encode(tmpBufferPtr, additions[0], pos);

                for (int i = 1; i < additions.Length; i++)
                {
                    if (pos + ZigZagEncoding.MaxEncodedSize >= tmpBuf.Length)
                    {
                        encoded = default;
                        return false;
                    }

                    long entry = additions[i] - additions[i - 1];
                    if (entry == 0)
                        continue; // we don't need to store duplicates

                    pos += ZigZagEncoding.Encode(tmpBufferPtr, entry, pos);
                }

                encoded = tmpBuf[..pos];
                return true;
            }
        }

        private void AddNewTerm(ref EntriesModifications entries, Span<byte> tmpBuf, out long termId)
        {
            var additions = entries.Additions;
            
            _numberOfTermModifications += 1;
            Debug.Assert(entries.TotalAdditions > 0, "entries.TotalAdditions > 0");
            // common for unique values (guid, date, etc)
            if (entries.TotalAdditions == 1)
            {
                entries.AssertPreparationIsNotFinished();
                termId = EntryIdEncodings.Encode(additions[0], entries.AdditionsFrequency[0], (long)TermIdMask.Single);                
                return;
            }

            entries.PrepareDataForCommiting();
            if (TryDeltaEncodingToBuffer(additions, tmpBuf, out var encoded) == false)
            {
                // too big, convert to a set
                AddNewTermToSet(additions, out termId);
                return;
            }

            // we'll increase the size of the allocation to 32 byte boundary. To make it cheaper to add to it in the future
            var allocatedSize = encoded.Length + 32 - (encoded.Length % 32);  
            var containerId = Container.Allocate(Transaction.LowLevelTransaction, _postingListContainerId, allocatedSize, out var space);
            encoded.CopyTo(space);

            termId = EntryIdEncodings.Encode(containerId, 0, TermIdMask.Small);
        }

        private unsafe void AddNewTermToSet(ReadOnlySpan<long> additions, out long termId)
        {
            long setId = Container.Allocate(Transaction.LowLevelTransaction, _postingListContainerId, sizeof(PostingListState), out var setSpace);
            ref var postingListState = ref MemoryMarshal.AsRef<PostingListState>(setSpace);
            PostingList.Create(Transaction.LowLevelTransaction, ref postingListState);

            PostingList.Update(Transaction.LowLevelTransaction, ref postingListState, additions, ReadOnlySpan<long>.Empty);
            termId = EntryIdEncodings.Encode(setId, 0, TermIdMask.Set);
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
            _jsonOperationContext?.Dispose();
            if (_ownsTransaction)
                Transaction?.Dispose();
            
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
        }
    }
}
