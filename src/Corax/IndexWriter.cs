using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Contracts;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Xml;
using Corax.Pipeline;
using Corax.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Compression;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Fixed;
using Voron.Data.Sets;
using Voron.Impl;

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
        private readonly Tree _indexMetadata;
        private readonly StorageEnvironment _environment;

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

        private unsafe struct EntriesModifications : IDisposable
        {
            private readonly ByteStringContext _context;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _disposable;

            private long* _start;
            private long* _end;
            private int _additions;
            private int _removals;
            private bool _sortingNeeded;

            public int TotalAdditions => _additions;
            public int TotalRemovals => _removals;

            public long TotalSpace => _end - _start;
            public long FreeSpace => TotalSpace - (_additions + _removals);

            private const int InitialSize = 16;

            public EntriesModifications([NotNull] ByteStringContext context)
            {
                _context = context;
                _disposable = _context.Allocate(InitialSize * sizeof(long), out var output);
                _start = (long*)output.Ptr;
                _end = _start + InitialSize;
                _additions = 0;
                _removals = 0;
                _sortingNeeded = false;
            }

            public void Addition(long entryId)
            {
                if (_additions > 0 && *(_start + _additions - 1) == entryId)
                    return;

                if (FreeSpace == 0)
                    GrowBuffer();

                *(_start + _additions) = entryId;
                _additions++;

                _sortingNeeded = true;
            }

            public void Removal(long entryId)
            {
                if (_removals > 0 && *(_end - _removals + 1) == entryId)
                    return;

                if (FreeSpace == 0)
                    GrowBuffer();

                *(_end - _removals) = entryId;
                _removals++;

                _sortingNeeded = true;
            }

            private void GrowBuffer()
            {
                int totalSpace = (int)TotalSpace;
                int newTotalSpace = totalSpace * 2;

                var scope = _context.Allocate(newTotalSpace * sizeof(long), out var output);
                long* start = (long*)output.Ptr;
                long* end = start + newTotalSpace;

                // Copy the contents that we already have.
                Unsafe.CopyBlockUnaligned(start, _start, (uint)_additions * sizeof(long));
                Unsafe.CopyBlockUnaligned(end - _removals + 1, _end - _removals + 1, (uint)_removals * sizeof(long));

                // Return the memory
                _disposable.Dispose();

                _start = start;
                _end = end;
                _disposable = scope;
            }

            public void Sort()
            {
                if (_removals + _additions <= 1)
                    _sortingNeeded = false;

                if (_sortingNeeded)
                {
                    MemoryExtensions.Sort(new Span<long>(_start, _additions));
                    MemoryExtensions.Sort(new Span<long>(_end - _removals + 1, _removals));
                    _sortingNeeded = false;
                }
                
                ValidateNoDuplicateEntries();
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

                foreach (var reomval in removals)
                {
                    if (additions.BinarySearch(reomval) >= 0)
                        throw new InvalidOperationException("Found duplicate addition & removal item during indexing: " + reomval);
                }
            }

            public ReadOnlySpan<long> Additions => new(_start, _additions);
            public ReadOnlySpan<long> Removals => new(_end - _removals + 1, _removals);
            
            public void Dispose()
            {
                _disposable.Dispose();
            }
        }
        
        private readonly Dictionary<Slice, EntriesModifications>[] _buffer;
        private readonly Dictionary<long, EntriesModifications>[] _bufferLongs;
        private readonly Dictionary<double, EntriesModifications>[] _bufferDoubles;
        private readonly HashSet<long> _deletedEntries = new();

        private readonly long _postingListContainerId, _entriesContainerId;

        private const string SuggestionsTreePrefix = "__Suggestion_";
        private Dictionary<int, Dictionary<Slice, int>> _suggestionsAccumulator;

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index writer with opening semantics and also every new
        // writer becomes essentially a unit of work which makes reusing assets tracking more explicit.

        private IndexWriter(IndexFieldsMapping fieldsMapping)
        {
            _fieldsMapping = fieldsMapping;
            fieldsMapping.UpdateMaximumOutputAndTokenSize();
            _encodingBufferHandler = Analyzer.BufferPool.Rent(fieldsMapping.MaximumOutputSize);
            _tokensBufferHandler = Analyzer.TokensPool.Rent(fieldsMapping.MaximumTokenSize);
            _utf8ConverterBufferHandler = Analyzer.BufferPool.Rent(fieldsMapping.MaximumOutputSize * 10);

            var bufferSize = fieldsMapping!.Count;
            _buffer = new Dictionary<Slice, EntriesModifications>[bufferSize];
            _bufferDoubles = new Dictionary<double, EntriesModifications>[bufferSize];
            _bufferLongs = new Dictionary<long, EntriesModifications>[bufferSize];
            for (int i = 0; i < bufferSize; ++i)
            {
                _buffer[i] = new Dictionary<Slice, EntriesModifications>(SliceComparer.Instance);
                _bufferDoubles[i] = new Dictionary<double, EntriesModifications>();
                _bufferLongs[i] = new Dictionary<long, EntriesModifications>();
            }
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
            _indexMetadata = Transaction.CreateTree(Constants.IndexMetadata);
        }

        public IndexWriter([NotNull] Transaction tx, IndexFieldsMapping fieldsMapping) : this(fieldsMapping)
        {
            Transaction = tx;

            _ownsTransaction = false;
            _postingListContainerId = Transaction.OpenContainer(Constants.IndexWriter.PostingListsSlice);
            _entriesContainerId = Transaction.OpenContainer(Constants.IndexWriter.EntriesContainerSlice);
            _indexMetadata = Transaction.CreateTree(Constants.IndexMetadata);
        }

        public long Index(string id, Span<byte> data)
        {
            using var _ = Slice.From(Transaction.Allocator, id, out var idSlice);
            return Index(idSlice, data);
        }

        public long Update(string field, Span<byte> key, LazyStringValue id, Span<byte> data, ref long numberOfEntries)
        {
            if (TryGetEntryTermId(field, key, out var entryId) == false)
            {
                numberOfEntries++;
                return Index(id, data);
            }
            // if there is more than a single entry for this key, delete & index from scratch
            // this is checked by calling code, but cheap to do this here as well.
            if((entryId & (long)TermIdMask.EnsureIsSingleMask) != 0)
            {
                RecordDeletion(entryId);
                numberOfEntries++;
                return Index(id, data);
            }

            Page lastVisitedPage = default;
            var oldEntryReader = IndexSearcher.GetReaderFor(Transaction, ref lastVisitedPage, entryId, out var rawSize);

            if (oldEntryReader.Buffer.SequenceEqual(data))
                return entryId; // no change, can skip all work here, joy!
           
            Span<byte> buf = stackalloc byte[10];
            var idLen = ZigZagEncoding.Encode(buf, id.Size);

            // can't fit in old size, have to remove anyway
            if (rawSize < idLen + id.Size + data.Length)
            {
                RecordDeletion(entryId);
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
          
            return entryId;
        }

        private void UpdateModifiedTermsOnly(ByteStringContext context, ref IndexEntryReader oldEntryReader, Span<byte> newEntryData,
            IndexFieldBinding fieldBinding, long entryId)
        {
            var newEntryReader = new IndexEntryReader(newEntryData);

            var oldType = oldEntryReader.GetFieldType(fieldBinding.FieldId, out var _);
            var newType = newEntryReader.GetFieldType(fieldBinding.FieldId, out var _);

            if (oldType != newType)
            {
                RemoveSingleTerm(fieldBinding, entryId, oldEntryReader, oldType);
                InsertToken(context, ref newEntryReader, entryId, fieldBinding);
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
                    bool oldHasIterator = oldEntryReader.TryReadMany(fieldBinding.FieldId, out var oldIterator);
                    bool newHasIterator = newEntryReader.TryReadMany(fieldBinding.FieldId, out var newIterator);
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
                        RemoveSingleTerm(fieldBinding, entryId, oldEntryReader, oldType);
                        InsertToken(context, ref newEntryReader, entryId, fieldBinding);
                    }
                    break;
                }
                case IndexEntryFieldType.Tuple:
                case IndexEntryFieldType.SpatialPoint:
                case IndexEntryFieldType.Simple:
                {
                    bool hasOld = oldEntryReader.Read(fieldBinding.FieldId, out var oldVal);
                    bool hasNew = newEntryReader.Read(fieldBinding.FieldId, out var newVal);
                    if (hasOld != hasNew || hasOld && oldVal.SequenceEqual(newVal) == false)
                    {
                        RemoveSingleTerm(fieldBinding, entryId, oldEntryReader, oldType);
                        InsertToken(context, ref newEntryReader, entryId, fieldBinding);
                    }
                    break;
                }
                case IndexEntryFieldType.Raw:
                case IndexEntryFieldType.RawList:
                case IndexEntryFieldType.Invalid:
                    break;

                case IndexEntryFieldType.SpatialPointList:
                {
                    bool oldHasIterator = oldEntryReader.TryReadManySpatialPoint(fieldBinding.FieldId, out var oldIterator);
                    bool newHasIterator = newEntryReader.TryReadManySpatialPoint(fieldBinding.FieldId, out var newIterator);
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
                        RemoveSingleTerm(fieldBinding, entryId, oldEntryReader, oldType);
                        InsertToken(context, ref newEntryReader, entryId, fieldBinding);
                    }
                    break;
                }
            }
        }

        public long Index(Slice id, Span<byte> data)
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
            var entryReader = new IndexEntryReader(data);

            foreach (var binding in _fieldsMapping)
            {
                if (binding.FieldIndexingMode is FieldIndexingMode.No)
                    continue;

                InsertToken(context, ref entryReader, entryId, binding);
            }

            return entryId;
        }

        public long GetNumberOfEntries() => (_indexMetadata.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0) + _numberOfModifications;

        private void AddSuggestions(IndexFieldBinding binding, Slice slice)
        {
            _suggestionsAccumulator ??= new Dictionary<int, Dictionary<Slice, int>>();

            if (_suggestionsAccumulator.TryGetValue(binding.FieldId, out var suggestionsToAdd) == false)
            {
                suggestionsToAdd = new Dictionary<Slice, int>();
                _suggestionsAccumulator[binding.FieldId] = suggestionsToAdd;
            }

            var keys = SuggestionsKeys.Generate(Transaction.Allocator, Constants.Suggestions.DefaultNGramSize, slice.AsSpan(), out int keysCount);
            int keySizes = keys.Length / keysCount;

            var bsc = Transaction.Allocator;

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

        private void RemoveSuggestions(IndexFieldBinding binding, ReadOnlySpan<byte> sequence)
        {
            if (_suggestionsAccumulator == null)
                _suggestionsAccumulator = new Dictionary<int, Dictionary<Slice, int>>();

            if (_suggestionsAccumulator.TryGetValue(binding.FieldId, out var suggestionsToAdd) == false)
            {
                suggestionsToAdd = new Dictionary<Slice, int>();
                _suggestionsAccumulator[binding.FieldId] = suggestionsToAdd;
            }

            var keys = SuggestionsKeys.Generate(Transaction.Allocator, Constants.Suggestions.DefaultNGramSize, sequence, out int keysCount);
            int keySizes = keys.Length / keysCount;

            var bsc = Transaction.Allocator;

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

        [SkipLocalsInit]
        private void InsertToken(ByteStringContext context, ref IndexEntryReader entryReader, long entryId,
            IndexFieldBinding binding)
        {
            var field = _buffer[binding.FieldId];
            var fieldLongs = _bufferLongs[binding.FieldId];
            var fieldDoubles = _bufferDoubles[binding.FieldId];
            int fieldId = binding.FieldId;
            var fieldType = entryReader.GetFieldType(fieldId, out var _);
            switch (fieldType)
            {
                case IndexEntryFieldType.Empty:
                case IndexEntryFieldType.Null:
                    var fieldName = fieldType == IndexEntryFieldType.Null ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                    ExactInsert(fieldName.AsReadOnlySpan());
                    break;
                
                case IndexEntryFieldType.TupleListWithNulls:
                case IndexEntryFieldType.TupleList:
                    if (entryReader.TryReadMany(binding.FieldId, out var iterator) == false)
                        break;

                    while (iterator.ReadNext())
                    {
                        if (iterator.IsNull)
                        {
                            ExactInsert(Constants.NullValueSlice);
                            NumericInsert(0L, double.NaN);
                        }
                        else if (iterator.IsEmpty)
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
                    if (entryReader.Read(binding.FieldId, out _,  out long lVal, out double dVal,out Span<byte> valueInEntry) == false)
                        break;

                    ExactInsert(valueInEntry);
                    NumericInsert(lVal, dVal);
                    break;

                case IndexEntryFieldType.SpatialPointList:
                    if (entryReader.TryReadManySpatialPoint(binding.FieldId, out var spatialIterator) == false)
                        break;

                    while (spatialIterator.ReadNext())
                    {
                        for (int i = 1; i <= spatialIterator.Geohash.Length; ++i)
                            ExactInsert(spatialIterator.Geohash.Slice(0, i));
                    }

                    break;

                case IndexEntryFieldType.SpatialPoint:
                    if (entryReader.Read(binding.FieldId, out valueInEntry) == false)
                        break;

                    for (int i = 1; i <= valueInEntry.Length; ++i)
                        ExactInsert(valueInEntry.Slice(0, i));

                    break;

                case IndexEntryFieldType.ListWithNulls:
                case IndexEntryFieldType.List:
                    if (entryReader.TryReadMany(binding.FieldId, out iterator) == false)
                        break;

                    while (iterator.ReadNext())
                    {
                        Debug.Assert((fieldType & IndexEntryFieldType.Tuple) == 0, "(fieldType & IndexEntryFieldType.Tuple) == 0");
                        
                        if ((fieldType & IndexEntryFieldType.HasNulls) != 0 && (iterator.IsEmpty || iterator.IsNull))
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
                    if (entryReader.Read(fieldId, out var value) == false)
                        break;

                    Insert(value);
                    break;
            }

            void Insert(ReadOnlySpan<byte> value)
            {
                if (binding.IsAnalyzed)
                    AnalyzeInsert(value);
                else
                    ExactInsert(value);
            }

            void AnalyzeInsert(ReadOnlySpan<byte> value)
            {
                var analyzer = binding.Analyzer;
                if (value.Length > _encodingBufferHandler.Length)
                {
                    analyzer.GetOutputBuffersSize(value.Length, out var outputSize, out var tokenSize);
                    if (outputSize > _encodingBufferHandler.Length || tokenSize >  _tokensBufferHandler.Length)
                        UnlikelyGrowBuffer(outputSize, tokenSize);
                }

                Span<byte> wordsBuffer = _encodingBufferHandler;
                Span<Token> tokens = _tokensBufferHandler;
                analyzer.Execute(value, ref wordsBuffer, ref tokens, ref _utf8ConverterBufferHandler);
                
                for (int i = 0; i < tokens.Length; i++)
                {
                    ref var token = ref tokens[i];

                    if (token.Offset + token.Length > _encodingBufferHandler.Length)
                        ThrowInvalidTokenFoundOnBuffer(binding, value, wordsBuffer, tokens, token);

                    var word = new Span<byte>(_encodingBufferHandler, token.Offset, (int)token.Length);
                    ExactInsert(word);
                }

                unsafe void ThrowInvalidTokenFoundOnBuffer(IndexFieldBinding binding, ReadOnlySpan<byte> value, Span<byte> wordsBuffer, Span<Token> tokens, Token token)
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
                        $"{Environment.NewLine}\tid: {binding.FieldId}" +
                        $"{Environment.NewLine}\tname: {binding.FieldName}");
                }
            }

            void NumericInsert(long lVal, double dVal)
            {
                // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
                ref var doublesTerms = ref CollectionsMarshal.GetValueRefOrAddDefault(fieldDoubles, dVal, out bool fieldDoublesExist);
                if (fieldDoublesExist == false)
                    doublesTerms = new EntriesModifications(Transaction.Allocator);
                doublesTerms.Addition(entryId);

                // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
                ref var longsTerms = ref CollectionsMarshal.GetValueRefOrAddDefault(fieldLongs, lVal, out bool fieldLongExist);
                if (fieldLongExist == false)
                    longsTerms = new EntriesModifications(Transaction.Allocator);
                longsTerms.Addition(entryId);
            }
            
            void ExactInsert(ReadOnlySpan<byte> value)
            {
                ByteStringContext<ByteStringMemoryCache>.InternalScope? scope;

                Slice slice;
                if (value.Length == 0)
                {
                    slice = Constants.EmptyStringSlice;
                    scope = null;
                }
                else
                {
                    scope = CreateNormalizedTerm(context, value, out slice);
                }

                // We are gonna try to get the reference if it exists, but we wont try to do the addition here, because to store in the
                // dictionary we need to close the slice as we are disposing it afterwards. 
                ref var term = ref CollectionsMarshal.GetValueRefOrAddDefault(field, slice, out var exists);
                if (exists == false)
                {
                    term = new EntriesModifications(context);
                    scope = null; // We don't want the fieldname (slice) to be returned.
                }

                term.Addition(entryId);

                if (binding.HasSuggestions)
                    AddSuggestions(binding, slice);

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
            var entryReader = IndexSearcher.GetReaderFor(Transaction, ref lastVisitedPage, entryToDelete, out var _);
            foreach (var binding in _fieldsMapping) // todo maciej: this part needs to be rebuilt after implementing DynamicFields
            {
                if (binding.IsIndexed == false)
                    continue;

                var fieldType = entryReader.GetFieldType(binding.FieldId, out _);
                RemoveSingleTerm(binding, entryToDelete, entryReader, fieldType);
            }

            Container.Delete(llt, _entriesContainerId, entryToDelete); // delete raw index entry
        }

        private void RemoveSingleTerm(IndexFieldBinding indexFieldBinding, long entryToDelete, IndexEntryReader entryReader, IndexEntryFieldType fieldType)
        {
            var context = Transaction.Allocator;

            switch (fieldType)
            {
                case IndexEntryFieldType.Empty:
                case IndexEntryFieldType.Null:
                    var termValue = fieldType == IndexEntryFieldType.Null ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                    RecordExactTermToDelete(termValue, indexFieldBinding);
                    break;
                case IndexEntryFieldType.TupleListWithNulls:
                case IndexEntryFieldType.TupleList:
                {
                    if (entryReader.TryReadMany(indexFieldBinding.FieldId, out var iterator) == false)
                        break;

                    while (iterator.ReadNext())
                    {
                        if (iterator.IsNull)
                        {
                            RecordTupleToDelete(indexFieldBinding, Constants.NullValueSlice, double.NaN, 0);
                        }
                        else if (iterator.IsEmpty)
                        {
                            throw new InvalidDataException("Tuple list cannot contain an empty string (otherwise, where did the numeric came from!)");
                        }
                        else
                        {
                            RecordTupleToDelete(indexFieldBinding, iterator.Sequence, iterator.Double, iterator.Long);
                        }
                    }

                    break;
                }
                case IndexEntryFieldType.Tuple:
                    if (entryReader.Read(indexFieldBinding.FieldId, out _, out long l, out double d, out Span<byte> valueInEntry) == false)
                        break;
                    RecordTupleToDelete(indexFieldBinding, valueInEntry, d, l);
                    break;

                case IndexEntryFieldType.SpatialPointList:
                    if (entryReader.TryReadManySpatialPoint(indexFieldBinding.FieldId, out var spatialIterator) == false)
                        break;

                    while (spatialIterator.ReadNext())
                    {
                        for (int i = 1; i <= spatialIterator.Geohash.Length; ++i)
                        {
                            var spatialTerm = spatialIterator.Geohash.Slice(0, i);
                            RecordExactTermToDelete(spatialTerm, indexFieldBinding);
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
                    if (entryReader.TryReadMany(indexFieldBinding.FieldId, out var iterator) == false)
                        break;

                    while (iterator.ReadNext())
                    {
                        if (iterator.IsNull)
                        {
                            RecordExactTermToDelete(Constants.NullValueSlice, indexFieldBinding);
                        }
                        else if (iterator.IsEmpty)
                        {
                            RecordExactTermToDelete(Constants.EmptyStringSlice, indexFieldBinding);
                        }
                        else
                        {
                            RecordTermToDelete(iterator.Sequence, indexFieldBinding);
                        }
                    }

                    break;
                }

                case IndexEntryFieldType.SpatialPoint:
                    if (entryReader.Read(indexFieldBinding.FieldId, out valueInEntry) == false)
                        break;

                    for (int i = 1; i <= valueInEntry.Length; ++i)
                    {
                        var spatialTerm = valueInEntry.Slice(0, i);
                        RecordExactTermToDelete(spatialTerm, indexFieldBinding);
                    }

                    break;
                default:
                    if (entryReader.Read(indexFieldBinding.FieldId, out var value) == false)
                        break;

                    if (value.IsEmpty)
                        goto case IndexEntryFieldType.Empty;

                    RecordTermToDelete(value, indexFieldBinding);
                    break;
            }

            void RecordTupleToDelete(IndexFieldBinding binding, ReadOnlySpan<byte> termValue, double termDouble, long termLong)
            {
                // Is there any reason to analyze string of number?
                RecordExactTermToDelete(termValue, binding);

                // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
                ref var doublesTerms = ref CollectionsMarshal.GetValueRefOrAddDefault(_bufferDoubles[binding.FieldId], termDouble, out bool fieldDoublesExist);
                if (fieldDoublesExist == false)
                    doublesTerms = new EntriesModifications(context);
                doublesTerms.Removal(entryToDelete);

                // We make sure we get a reference because we want the struct to be modified directly from the dictionary.
                ref var longsTerms = ref CollectionsMarshal.GetValueRefOrAddDefault(_bufferLongs[binding.FieldId], termLong, out bool fieldLongExist);
                if (fieldLongExist == false)
                    longsTerms = new EntriesModifications(context);
                longsTerms.Removal(entryToDelete);
            }

            void RecordTermToDelete(ReadOnlySpan<byte> termValue, IndexFieldBinding binding)
            {
                if (binding.HasSuggestions)
                    RemoveSuggestions(binding, termValue);

                if (binding.IsAnalyzed == false)
                {
                    RecordExactTermToDelete(termValue, binding);
                    return;
                }
                
                var analyzer = binding.Analyzer;
                if (termValue.Length > _encodingBufferHandler.Length)
                {
                    analyzer.GetOutputBuffersSize(termValue.Length, out int outputSize, out int tokenSize);
                    if (outputSize > _encodingBufferHandler.Length || tokenSize > _tokensBufferHandler.Length)
                        UnlikelyGrowBuffer(outputSize, tokenSize);
                }

                var tokenSpace = _tokensBufferHandler.AsSpan();
                var wordSpace = _encodingBufferHandler.AsSpan();
                analyzer.Execute(termValue, ref wordSpace, ref tokenSpace, ref _utf8ConverterBufferHandler);

                for (int i = 0; i < tokenSpace.Length; i++)
                {
                    ref var token = ref tokenSpace[i];

                    var term = wordSpace.Slice(token.Offset, (int)token.Length);
                    RecordExactTermToDelete(term, binding);
                }
            }
            
            void RecordExactTermToDelete(ReadOnlySpan<byte> termValue, IndexFieldBinding binding)
            {
                ByteStringContext<ByteStringMemoryCache>.InternalScope? scope = CreateNormalizedTerm(context, termValue, out Slice termSlice);

                // We are gonna try to get the reference if it exists, but we wont try to do the addition here, because to store in the
                // dictionary we need to close the slice as we are disposing it afterwards. 
                ref var term = ref CollectionsMarshal.GetValueRefOrAddDefault(_buffer[binding.FieldId], termSlice, out var exists);
                if (exists == false)
                {
                    term = new EntriesModifications(context);
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
                RecordTermsToDeleteFrom(entryToDelete, llt, ref lastVisitedPage);
            }
        }

        public bool TryDeleteEntry(string key, string term)
        {
            using var _ = Slice.From(Transaction.Allocator, term, ByteStringType.Immutable, out var termSlice);
            return TryDeleteEntry(key, termSlice.AsSpan());
        }

        public bool TryDeleteEntry(string key, Span<byte> term)
        {
            if (!TryGetEntryTermId(key, term, out long idInTree)) 
                return false;

            RecordDeletion(idInTree);
            return true;
        }

        private void RecordDeletion(long idInTree)
        {
            if ((idInTree & (long)TermIdMask.Set) != 0)
            {
                var id = idInTree & Constants.StorageMask.ContainerType;
                var setSpace = Container.GetMutable(Transaction.LowLevelTransaction, id);
                ref var setState = ref MemoryMarshal.AsRef<SetState>(setSpace);
                var set = new Set(Transaction.LowLevelTransaction, Slices.Empty, setState);
                var iterator = set.Iterate();
                while (iterator.MoveNext())
                {
                    _deletedEntries.Add(iterator.Current);
                    _numberOfModifications--;
                }
            }
            else if ((idInTree & (long)TermIdMask.Small) != 0)
            {
                var id = idInTree & Constants.StorageMask.ContainerType;
                var smallSet = Container.Get(Transaction.LowLevelTransaction, id).ToSpan();
                // combine with existing value
                var cur = 0L;
                var count = ZigZagEncoding.Decode<int>(smallSet, out var pos);
                for (int idX = 0; idX < count; ++idX)
                {
                    var value = ZigZagEncoding.Decode<long>(smallSet, out var len, pos);
                    pos += len;
                    cur += value;
                    _deletedEntries.Add(cur);
                    _numberOfModifications--;
                }
            }
            else
            {
                _deletedEntries.Add(idInTree);
                _numberOfModifications--;
            }
        }

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
            return fieldTree.TryGetValue(termValue, out idInTree, out var _);
        }

        private void UnlikelyGrowBuffer(int newBufferSize, int newTokenSize)
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

        public void Commit()
        {
            using var _ = Transaction.Allocator.Allocate(Container.MaxSizeInsideContainerPage, out Span<byte> workingBuffer);
            Tree fieldsTree = Transaction.CreateTree(Constants.IndexWriter.FieldsSlice);
            
            _indexMetadata.Increment(Constants.IndexWriter.NumberOfEntriesSlice, _numberOfModifications);

            ProcessDeletes();

            Slice[] keys = Array.Empty<Slice>();
            
            for (int fieldId = 0; fieldId < _fieldsMapping.Count; ++fieldId)
            {
                if (_buffer[fieldId].Count == 0)
                    continue; 

                InsertTextualField(fieldsTree, fieldId, workingBuffer, ref keys);
                InsertNumericFieldLongs(fieldsTree, fieldId, workingBuffer);
                InsertNumericFieldDoubles(fieldsTree, fieldId, workingBuffer);
            }
            
            if(keys.Length>0)
                ArrayPool<Slice>.Shared.Return(keys);

            // Check if we have suggestions to deal with. 
            if (_suggestionsAccumulator != null)
            {
                foreach (var (fieldId, values) in _suggestionsAccumulator)
                {
                    Slice.From(Transaction.Allocator, $"{SuggestionsTreePrefix}{fieldId}", out var treeName);
                    var tree = Transaction.CompactTreeFor(treeName);
                    foreach (var (key, counter) in values)
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


        private void InsertTextualField(Tree fieldsTree, int fieldId, Span<byte> tmpBuf, ref Slice[] sortedTermsBuffer)
        {
            var fieldTree = fieldsTree.CompactTreeFor(_fieldsMapping.GetByFieldId(fieldId).FieldName);

            var currentFieldTerms = _buffer[fieldId];
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

            using var dumper = new IndexTermDumper(fieldsTree, fieldId);

            fieldTree.InitializeStateForTryGetNextValue();
            for (var index = 0; index < termsCount; index++)
            {
                var term = sortedTermsBuffer[index];

                ref var entries = ref CollectionsMarshal.GetValueRefOrNullRef(currentFieldTerms, term);
                Debug.Assert(Unsafe.IsNullRef(ref entries) == false);

                long termId;
                ReadOnlySpan<byte> termsSpan = term.AsSpan();
                if (fieldTree.TryGetNextValue(termsSpan, out var existing, out var encodedKey) == false)
                {
                    if (entries.TotalRemovals != 0)
                    {
                        throw new InvalidOperationException($"Attempt to remove entries from new term: '{term}' for field {fieldId}! This is a bug.");
                    }

                    AddNewTerm(entries, tmpBuf, out termId);

                    dumper.WriteAddition(term, termId);
                    fieldTree.Add(termsSpan, termId, encodedKey);
                    continue;
                }

                switch (AddEntriesToTerm(tmpBuf, existing, ref entries, out termId))
                {
                    case AddEntriesToTermResult.UpdateTermId:
                        dumper.WriteAddition(term, termId);
                        fieldTree.Add(termsSpan, termId, encodedKey);
                        break;
                    case AddEntriesToTermResult.RemoveTermId:
                        if (fieldTree.TryRemove(termsSpan, out var ttt) == false)
                        {
                            dumper.WriteRemoval(term, termId);
                            throw new InvalidOperationException($"Attempt to remove term: '{term}' for field {fieldId}, but it does not exists! This is a bug.");
                        }
                        dumper.WriteRemoval(term, ttt);
                        break;
                }
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
            var llt = Transaction.LowLevelTransaction;

            var smallSet = Container.GetMutable(llt, id);
            Debug.Assert(entries.Removals.ToArray().Distinct().Count() == entries.TotalRemovals, $"Removals list is not distinct.");
            
            entries.Sort();
          
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
                
                if (removalIndex < removals.Length)
                {
                    if (currentId == removals[removalIndex])
                    {
                        removalIndex++;
                        continue;
                    }

                    if (currentId > removals[removalIndex])
                        throw new InvalidDataException("Attempt to remove value " + removals[removalIndex] + ", but got " + currentId);
                }

                entries.Addition(currentId);

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

            if (entries.TotalAdditions == 0)
            {
                Container.Delete(llt, _postingListContainerId, id);
                termId = -1;
                return AddEntriesToTermResult.RemoveTermId;
            }

            entries.Sort();

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
            termId |= (long)TermIdMask.Small;
            
            encoded.CopyTo(space);
            return AddEntriesToTermResult.UpdateTermId;
        }

        private AddEntriesToTermResult AddEntriesToTermResultSingleValue(Span<byte> tmpBuf, long existing, ref EntriesModifications entries, out long termId)
        {
            // single
            if (entries.TotalAdditions == 1 && entries.Additions[0] == existing && entries.TotalRemovals == 0)
            {
                // Same element to add, nothing to do here.
                termId = -1;
                return AddEntriesToTermResult.NothingToDo;
            }

            if (entries.TotalRemovals != 0)
            {
                if (entries.Removals[0] != existing || entries.TotalRemovals != 1)
                    throw new InvalidDataException($"Attempt to delete id {string.Join(", ", entries.Removals.ToArray())} that does not exists, only value is: {existing}");

                if (entries.TotalAdditions == 0)
                {
                    termId = -1;
                    return AddEntriesToTermResult.RemoveTermId;
                }
            }
            else
            {
                entries.Addition(existing);
            }
            
            AddNewTerm(entries, tmpBuf, out termId);
            return AddEntriesToTermResult.UpdateTermId;
        }

        private AddEntriesToTermResult AddEntriesToTermResultViaLargeSet(ref EntriesModifications entries, out long termId, long id)
        {
            var llt = Transaction.LowLevelTransaction;

            var setSpace = Container.GetMutable(llt, id);
            ref var setState = ref MemoryMarshal.AsRef<SetState>(setSpace);
            var set = new Set(llt, Slices.Empty, setState);
            entries.Sort();

            set.Remove(entries.Removals);
            set.Add(entries.Additions);

            termId = -1;

            if (set.State.NumberOfEntries == 0)
            {
                llt.FreePage(set.State.RootPage);
                Container.Delete(llt, _postingListContainerId, id);
                return AddEntriesToTermResult.RemoveTermId;
            }

            setState = set.State;
            return AddEntriesToTermResult.NothingToDo;
        }

        private unsafe void InsertNumericFieldLongs(Tree fieldsTree, int fieldId, Span<byte> tmpBuf)
        {
            FixedSizeTree fieldTree = fieldsTree.FixedTreeFor(_fieldsMapping.GetByFieldId(fieldId).FieldNameLong, sizeof(long));
          
            foreach (var (term, entries) in _bufferLongs[fieldId])
            {
                // We are not going to be using these entries anymore after this. 
                // Therefore, we can copy and we dont need to get a reference to the entry in the dictionary.
                // IMPORTANT: No modification to the dictionary can happen from this point onwards. 
                var localEntry = entries;

                long termId;

                using var _ = fieldTree.Read(term, out var result);
                if (result.HasValue == false)
                {
                    Debug.Assert(localEntry.TotalRemovals == 0, "entries.TotalRemovals == 0");
                    AddNewTerm(localEntry, tmpBuf, out termId);
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
        
        private unsafe void InsertNumericFieldDoubles(Tree fieldsTree, int fieldId, Span<byte> tmpBuf)
        {
            var fieldTree = fieldsTree.FixedTreeForDouble(_fieldsMapping.GetByFieldId(fieldId).FieldNameDouble, sizeof(long));

            foreach (var (term, entries) in _bufferDoubles[fieldId])
            {
                // We are not going to be using these entries anymore after this. 
                // Therefore, we can copy and we dont need to get a reference to the entry in the dictionary.
                // IMPORTANT: No modification to the dictionary can happen from this point onwards. 
                var localEntry = entries;

                using var _ = fieldTree.Read(term, out var result);

                long termId;
                if (result.Size == 0) // no existing value
                {
                    Debug.Assert(localEntry.TotalRemovals == 0, "entries.TotalRemovals == 0");
                    AddNewTerm(localEntry, tmpBuf, out termId);
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

        private void AddNewTerm(in EntriesModifications entries, Span<byte> tmpBuf, out long termId, bool sortingNeeded = true)
        {
            var additions = entries.Additions;

            Debug.Assert(entries.TotalAdditions > 0, "entries.TotalAdditions > 0");
            // common for unique values (guid, date, etc)
            if (entries.TotalAdditions == 1)
            {
                termId = additions[0] | (long)TermIdMask.Single;                
                return;
            }

            // Because the sorting would not change the struct itself, it is safe to use an 'in' modifier to avoid the copying. 
            if(sortingNeeded)
                entries.Sort();

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

            termId = containerId | (long)TermIdMask.Small;
        }

        private unsafe void AddNewTermToSet(ReadOnlySpan<long> additions, out long termId)
        {
            long setId = Container.Allocate(Transaction.LowLevelTransaction, _postingListContainerId, sizeof(SetState), out var setSpace);
            ref var setState = ref MemoryMarshal.AsRef<SetState>(setSpace);
            Set.Create(Transaction.LowLevelTransaction, ref setState);
            var set = new Set(Transaction.LowLevelTransaction, Slices.Empty, setState);
            set.Add(additions);
            setState = set.State;
            termId = setId | (long)TermIdMask.Set;
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
