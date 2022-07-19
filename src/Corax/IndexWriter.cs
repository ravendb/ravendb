using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Pipeline;
using Corax.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Compression;
using Voron;
using Voron.Data.BTrees;
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
        Small = 1,
        Set = 2
    }

    public class IndexWriter : IDisposable // single threaded, controlled by caller
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

        private readonly Dictionary<Slice, List<long>>[] _buffer;
        private readonly Dictionary<long, List<long>>[] _bufferLongs;
        private readonly Dictionary<double, List<long>>[] _bufferDoubles;
        private HashSet<long> _deletedEntries = new();

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
            _buffer = new Dictionary<Slice, List<long>>[bufferSize];
            _bufferDoubles = new Dictionary<double, List<long>>[bufferSize];
            _bufferLongs = new Dictionary<long, List<long>>[bufferSize];
            for (int i = 0; i < bufferSize; ++i)
            {
                _buffer[i] = new Dictionary<Slice, List<long>>(SliceComparer.Instance);
                _bufferDoubles[i] = new Dictionary<double, List<long>>();
                _bufferLongs[i] = new Dictionary<long, List<long>>();
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
        
        public long Index(Slice id, Span<byte> data)
        {
            _numberOfModifications++;
            Span<byte> buf = stackalloc byte[10];
            var idLen = ZigZagEncoding.Encode(buf, id.Size);
            var entryId = Container.Allocate(Transaction.LowLevelTransaction, _entriesContainerId, idLen + id.Size + data.Length, out var space);
            buf.Slice(0, idLen).CopyTo(space);
            space = space.Slice(idLen);
            id.CopyTo(space);
            space = space.Slice(id.Size);
            data.CopyTo(space);

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

        private unsafe void AddSuggestions(IndexFieldBinding binding, Slice slice)
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void AddMaybeAvoidDuplicate(List<long> term, long entryId)
        {
            if (term.Count > 0 && term[^1] == entryId)
                return;

            term.Add(entryId);
        }

        [SkipLocalsInit]
        private unsafe void InsertToken(ByteStringContext context, ref IndexEntryReader entryReader, long entryId,
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

                case IndexEntryFieldType.TupleList:
                    if (entryReader.TryReadMany(binding.FieldId, out var iterator) == false)
                        break;

                    while (iterator.ReadNext())
                    {
                        ExactInsert(iterator.Sequence);
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

                case IndexEntryFieldType.TupleListWithNulls:
                case IndexEntryFieldType.ListWithNulls:
                case IndexEntryFieldType.List:
                    if (entryReader.TryReadMany(binding.FieldId, out iterator) == false)
                        break;

                    while (iterator.ReadNext())
                    {
                        if ((fieldType & IndexEntryFieldType.HasNulls) != 0 && (iterator.IsEmpty || iterator.IsNull))
                        {
                            var fieldValue = iterator.IsNull ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                            ExactInsert(fieldValue.AsReadOnlySpan());
                        }
                        else if ((fieldType & IndexEntryFieldType.Tuple) != 0)
                        {
                            NumericInsert(iterator.Long, iterator.Double);
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
                        $"{Environment.NewLine}Got token with: {Environment.NewLine}\tOFFSET {token.Offset}{Environment.NewLine}\tLENGTH: {token.Length}.{Environment.NewLine}" +
                        $"Total amount of tokens: {tokens.Length}" +
                        $"{Environment.NewLine}Buffer contains '{Encodings.Utf8.GetString(wordsBuffer)}' and total length is {wordsBuffer.Length}" +
                        $"{Environment.NewLine}Buffer from ArrayPool: {Environment.NewLine}\tbyte buffer is {_encodingBufferHandler.Length} {Environment.NewLine}\ttokens buffer is {_tokensBufferHandler.Length}" +
                        $"{Environment.NewLine}Original span cointains '{Encodings.Utf8.GetString(value)}' with total length {value.Length}" +
                        $"{Environment.NewLine}Field " +
                        $"{Environment.NewLine}\tid: {binding.FieldId}" +
                        $"{Environment.NewLine}\tname: {binding.FieldName}");
                }
            }

            void NumericInsert(long lVal, double dVal)
            {
                if (fieldDoubles.TryGetValue(dVal, out var doublesTerms) == false)
                    fieldDoubles[dVal] = doublesTerms = new List<long>();
                if(fieldLongs.TryGetValue(lVal, out var longsTerms) == false)
                    fieldLongs[lVal] = longsTerms = new List<long>();

                AddMaybeAvoidDuplicate(doublesTerms, entryId);
                AddMaybeAvoidDuplicate(longsTerms, entryId);
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

                if (field.TryGetValue(slice, out var term) == false)
                {
                    var fieldName = slice.Clone(context);
                    field[fieldName] = term = new List<long>();
                }

                AddMaybeAvoidDuplicate(term, entryId);

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
                Debug.Assert(Constants.Terms.MaxLength == hashStartingPoint + hexSize);

                return Slice.From(context, localValue, ByteStringType.Mutable, out slice);
            }
            else
            {
                return Slice.From(context, value, ByteStringType.Mutable, out slice);
            }
        }

        private void DeleteEntry(long entryToDelete, Tree fieldsTree,  LowLevelTransaction llt, ref Page lastVisitedPage)
        {
            var entryReader = IndexSearcher.GetReaderFor(Transaction, ref lastVisitedPage, entryToDelete);
            foreach (var binding in _fieldsMapping) // todo maciej: this part needs to be rebuilt after implementing DynamicFields
            {
                if (binding.IsIndexed == false)
                    continue;

                var fieldType = entryReader.GetFieldType(binding.FieldId, out _);
                switch (fieldType)
                {
                    case IndexEntryFieldType.Empty:
                    case IndexEntryFieldType.Null:
                        var fieldName = fieldType == IndexEntryFieldType.Null ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                        DeleteIdFromExactTerm(entryToDelete, binding.FieldName, fieldName.AsReadOnlySpan(),fieldsTree, llt);
                        break;

                    case IndexEntryFieldType.TupleList:
                        if (entryReader.TryReadMany(binding.FieldId, out var iterator) == false)
                            break;

                        while (iterator.ReadNext())
                        {
                            DeleteIdFromExactTerm(entryToDelete, binding.FieldName, iterator.Sequence,  fieldsTree, llt);
                            DeleteIdFromNumericTerm(entryToDelete, binding.FieldNameDouble, iterator.Double, fieldsTree,llt);
                            DeleteIdFromNumericTerm(entryToDelete, binding.FieldNameLong, iterator.Long, fieldsTree, llt);
                        }

                        break;

                    case IndexEntryFieldType.Tuple:
                        if (entryReader.Read(binding.FieldId, out _, out long l, out double d, out Span<byte> valueInEntry) == false)
                            break;
                        DeleteIdFromExactTerm(entryToDelete, binding.FieldName, valueInEntry, fieldsTree, llt);
                        DeleteIdFromNumericTerm(entryToDelete, binding.FieldNameDouble, d, fieldsTree, llt);
                        DeleteIdFromNumericTerm(entryToDelete, binding.FieldNameLong, l, fieldsTree, llt);
                        break;

                    case IndexEntryFieldType.SpatialPointList:
                        if (entryReader.TryReadManySpatialPoint(binding.FieldId, out var spatialIterator) == false)
                            break;

                        while (spatialIterator.ReadNext())
                        {
                            for (int i = 1; i <= spatialIterator.Geohash.Length; ++i)
                            {
                                var readOnlySpan = spatialIterator.Geohash.Slice(0, i);
                                DeleteIdFromExactTerm(entryToDelete, binding.FieldName, readOnlySpan,fieldsTree, llt);
                            }
                        }

                        break;
                    case IndexEntryFieldType.Raw:
                    case IndexEntryFieldType.RawList:
                    case IndexEntryFieldType.Invalid:
                        break;
                    default:
                        if (entryReader.Read(binding.FieldId, out var value) == false)
                            break;

                        DeleteIdFromTerm(value, entryToDelete, binding, fieldsTree, llt);
                        break;
                }
            }

            Container.Delete(llt, _entriesContainerId, entryToDelete); // delete raw index entry
        }
    
        void DeleteIdFromTerm(ReadOnlySpan<byte> termValue, long entryToDelete,  IndexFieldBinding binding,
            Tree fieldsTree, LowLevelTransaction llt)
        {
            if (binding.IsAnalyzed == false)
            {
                DeleteIdFromExactTerm(entryToDelete, binding.FieldName, termValue, fieldsTree, llt);
                if (binding.HasSuggestions)
                    RemoveSuggestions(binding, termValue);

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
                DeleteIdFromExactTerm(entryToDelete, binding.FieldName, term,  fieldsTree, llt);

                if (binding.HasSuggestions)
                    RemoveSuggestions(binding, termValue);
            }
        }


        unsafe void DeleteIdFromNumericTerm<TVal>(long idToDelete, Slice fieldName, TVal val,
            Tree fieldsTree, LowLevelTransaction llt)
            where TVal : unmanaged, IBinaryNumber<TVal>, IMinMaxValue<TVal>
        {
            var fieldTree = fieldsTree.FixedSizeTree<TVal>(fieldsTree, fieldName, (byte)sizeof(TVal));

            using var _ = fieldTree.Read(val, out var result);
            if (result.HasValue == false)
                return;

            var containerId = *((long*)result.Content.Ptr);
            var newContainerId = RemoveValue(containerId, fieldName, idToDelete, llt);
            if (newContainerId == null || newContainerId.Value != containerId)
                fieldTree.Delete(val);
            if (newContainerId != null)
                fieldTree.Add(val, newContainerId.Value);
        }

        void DeleteIdFromExactTerm(long idToDelete, Slice fieldName, ReadOnlySpan<byte> termValue, 
            Tree fieldsTree, LowLevelTransaction llt)
        {
            // We need to normalize the term in case we have a term bigger than MaxTermLength.
            using var _ = CreateNormalizedTerm(Transaction.Allocator, termValue, out Slice termSlice);
            termValue = termSlice.AsReadOnlySpan();

            var fieldTree = fieldsTree.CompactTreeFor(fieldName);
            if (termValue.Length == 0 || fieldTree.TryGetValue(termSlice.AsReadOnlySpan(), out var containerId) == false)
                return;

            var newContainerId = RemoveValue(containerId, fieldName, idToDelete, llt);
            if (newContainerId == null || newContainerId.Value != containerId)
                fieldTree.TryRemove(termValue, out var __);

            if (newContainerId != null)
            {
                fieldTree.Add(termValue, newContainerId.Value);
            }
        }
    
        long? RemoveValue(long containerId, Slice fieldName, long idToDelete, LowLevelTransaction llt)
        {
            if ((containerId & (long)TermIdMask.Set) != 0)
            {
                var setId = containerId & ~0b11;
                var setStateSpan = Container.GetMutable(llt, setId);
                ref var setState = ref MemoryMarshal.AsRef<SetState>(setStateSpan);
                var set = new Set(llt, fieldName, in setState);
                set.Remove(idToDelete);
                setState = set.State;

                if (setState.NumberOfEntries == 0)
                {
                    //If we get rid off all terms we have to remove container. Probably we can do this in a better way
                    Container.Delete(llt, _postingListContainerId, setId);
                    return null;
                }

                return containerId;
            }

            if ((containerId & (long)TermIdMask.Small) != 0)
            {
                var smallSetId = containerId & ~0b11;
                var buffer = Container.GetMutable(llt, smallSetId);

                //Fetch all the ids from the set into temporaryStorageForIds
                var itemsCount = ZigZagEncoding.Decode<int>(buffer, out var len);
                //we assume that the entry contains the value
                var writePos = ZigZagEncoding.Encode(buffer, itemsCount - 1);
                if (itemsCount == 1)
                { // last one
                    Container.Delete(llt, _postingListContainerId, smallSetId);
                    return null;
                }
                long readPos = len;
                var idx = 0;
                var currentId = 0L;
                var lastWrite = 0L;

                while (idx++ < itemsCount)
                {
                    var delta = ZigZagEncoding.Decode<long>(buffer, out len, (int)readPos);
                    readPos += len;
                    currentId += delta;
                    if (currentId == idToDelete)
                        continue;
                    writePos += ZigZagEncoding.Encode(buffer, currentId - lastWrite, writePos);
                    lastWrite = currentId;
                }

                return containerId;
            }

            return null;
        }

        private void DeleteCommit(Tree fieldsTree)
        {
            if (_deletedEntries.Count == 0)
                return;
            var llt = Transaction.LowLevelTransaction;
            Page lastVisitedPage = default;
            foreach (long entryToDelete in _deletedEntries)
            {
                DeleteEntry(entryToDelete, fieldsTree, llt, ref lastVisitedPage);
            }
        }

        public bool TryDeleteEntry(string key, string term)
        {
            var fieldsTree = Transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
            if (fieldsTree == null)
                return false;

            var fieldTree = fieldsTree.CompactTreeFor(key);

            // We need to normalize the term in case we have a term bigger than MaxTermLength.
            using var _ = Slice.From(Transaction.Allocator, term, out var termSlice);
            using var __ = CreateNormalizedTerm(Transaction.Allocator, termSlice.AsReadOnlySpan(), out termSlice);

            var termValue = termSlice.AsReadOnlySpan();
            if (fieldTree.TryGetValue(termValue, out long idInTree, out var _) == false)
                return false;

            if ((idInTree & (long)TermIdMask.Set) != 0)
            {
                var id = idInTree & ~0b11;
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
                var id = idInTree & ~0b11;
                var smallSet = Container.Get(Transaction.LowLevelTransaction, id).ToSpan();
                // combine with existing value
                var cur = 0L;
                var count = ZigZagEncoding.Decode<int>(smallSet, out var pos);
                var idx = 0;
                while (idx++ < count)
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

            return true;
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

            if (_fieldsMapping.Count != 0)
                DeleteCommit(fieldsTree);

            for (int fieldId = 0; fieldId < _fieldsMapping.Count; ++fieldId)
            {
                InsertTextualField(fieldsTree, fieldId, workingBuffer);
                InsertNumericFieldLongs(fieldsTree, fieldId, workingBuffer);
                InsertNumericFieldDoubles(fieldsTree, fieldId, workingBuffer);
            }

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

        private void InsertTextualField(Tree fieldsTree, int fieldId, Span<byte> tmpBuf)
        {
            var fieldTree = fieldsTree.CompactTreeFor(_fieldsMapping.GetByFieldId(fieldId).FieldName);
            var llt = Transaction.LowLevelTransaction;

            foreach (var (term, entries) in _buffer[fieldId])
            {
                long termId;
                ReadOnlySpan<byte> termsSpan = term.AsSpan();
                if (fieldTree.TryGetValue(termsSpan, out var existing, out var encodedKey) == false)
                {
                    if (AddNewTerm(entries, tmpBuf, out termId))
                        fieldTree.Add(termsSpan, termId, encodedKey);
                    continue;
                }

                if (AddEntriesToTerm(tmpBuf, existing, llt, entries, out termId))
                    fieldTree.Add(termsSpan, termId, encodedKey);
            }
        }

        private bool AddEntriesToTerm(Span<byte> tmpBuf, long existing, LowLevelTransaction llt, List<long> entries, out long termId)
        {
            if ((existing & (long)TermIdMask.Set) != 0)
            {
                var id = existing & ~0b11;
                var setSpace = Container.GetMutable(llt, id);
                ref var setState = ref MemoryMarshal.AsRef<SetState>(setSpace);
                var set = new Set(llt, Slices.Empty, setState);
                entries.Sort();
                set.Add(entries);
                setState = set.State;
                termId = -1;
                return false; // it's already there, no need
            }
            else
            {
                if ((existing & (long)TermIdMask.Small) != 0)
                {
                    var id = existing & ~0b11;
                    var smallSet = Container.Get(llt, id).ToSpan();
                    // combine with existing value
                    var cur = 0L;
                    var count = ZigZagEncoding.Decode<int>(smallSet, out var pos);
                    var idx = 0;
                    while (idx++ < count)
                    {
                        var value = ZigZagEncoding.Decode<long>(smallSet, out var len, pos);
                        pos += len;
                        cur += value;
                        entries.Add(cur);
                    }

                    Container.Delete(llt, _postingListContainerId, id);
                    return AddNewTerm(entries, tmpBuf, out termId);
                }
                // single
                
                if (entries.Count == 1 && entries[0] == existing)
                {
                    // Same element to add, nothing to do here.
                    termId = -1;
                    return false;
                }

                entries.Add(existing);
                return AddNewTerm(entries, tmpBuf, out termId);
            }
        }

        private unsafe void InsertNumericFieldLongs(Tree fieldsTree, int fieldId, Span<byte> tmpBuf)
        {
            FixedSizeTree fieldTree = fieldsTree.FixedTreeFor(_fieldsMapping.GetByFieldId(fieldId).FieldNameLong, sizeof(long));
            var llt = Transaction.LowLevelTransaction;
          
            foreach (var (term, entries) in _bufferLongs[fieldId])
            {
                long termId;

                using var _ = fieldTree.Read(term, out var result);
                if (result.HasValue == false)
                {
                    if (AddNewTerm(entries, tmpBuf, out termId))
                        fieldTree.Add(term, termId);
                    continue;
                }

                var existing = *((long*)result.Content.Ptr);
                if (AddEntriesToTerm(tmpBuf, existing, llt, entries, out termId))
                    fieldTree.Add(term, termId);
            }
        }
        
        private unsafe void InsertNumericFieldDoubles(Tree fieldsTree, int fieldId, Span<byte> tmpBuf)
        {
            var fieldTree = fieldsTree.FixedTreeForDouble(_fieldsMapping.GetByFieldId(fieldId).FieldNameDouble, sizeof(long));
            var llt = Transaction.LowLevelTransaction;

            foreach (var (term, entries) in _bufferDoubles[fieldId])
            {
                using var _ = fieldTree.Read(term, out var result);

                long termId;
                if (result.Size == 0) // no existing value
                {
                    if (AddNewTerm(entries, tmpBuf, out termId))
                        fieldTree.Add(term, termId);
                    continue;
                }

                var existing = *((long*)result.Content.Ptr);
                if (AddEntriesToTerm(tmpBuf, existing, llt, entries, out termId))
                    fieldTree.Add(term, termId);
            }
        }

        private unsafe bool AddNewTerm(List<long> entries, Span<byte> tmpBuf, out long termId, bool sortingNeeded = true)
        {
            if (entries.Count == 0)
            {
                termId = -1;
                return false;
            }

            // common for unique values (guid, date, etc)
            if (entries.Count == 1)
            {
                termId = entries[0] | (long)TermIdMask.Single;                
                return true;
            }

            if(sortingNeeded)
                entries.Sort();

            // try to insert to container value
            //TODO: using simplest delta encoding, need to do better here
            int pos = ZigZagEncoding.Encode(tmpBuf, entries.Count);
            pos += ZigZagEncoding.Encode(tmpBuf, entries[0], pos);
            var llt = Transaction.LowLevelTransaction;
            for (int i = 1; i < entries.Count; i++)
            {
                if (pos + ZigZagEncoding.MaxEncodedSize < tmpBuf.Length)
                {
                    long entry = entries[i] - entries[i - 1];
                    if (entry == 0)
                        continue; // we don't need to store duplicates

                    pos += ZigZagEncoding.Encode(tmpBuf, entry, pos);
                    continue;
                }

                // too big, convert to a set
                long setId = Container.Allocate(llt, _postingListContainerId, sizeof(SetState), out var setSpace);
                ref var setState = ref MemoryMarshal.AsRef<SetState>(setSpace);
                Set.Create(llt, ref setState);
                var set = new Set(llt, Slices.Empty, setState);
                entries.Sort();
                set.Add(entries);
                setState = set.State;
                termId = setId | (long)TermIdMask.Set;                
                return true;
            }

            var containerId = Container.Allocate(llt, _postingListContainerId, pos, out var space);
            tmpBuf.Slice(0, pos).CopyTo(space);

            termId = containerId | (long)TermIdMask.Small;
            return true;
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
