using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
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
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
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
        private readonly IndexFieldsMapping _fieldsMapping;

        private readonly StorageEnvironment _environment;

        private readonly bool _ownsTransaction;
        private JsonOperationContext _jsonOperationContext;
        public readonly Transaction Transaction;
        private readonly TransactionPersistentContext _transactionPersistentContext;

        private Token[] _tokensBufferHandler;
        private byte[] _encodingBufferHandler;

        // CPU bound - embarassingly parallel
        // 
        // private readonly ConcurrentDictionary<Slice, Dictionary<Slice, ConcurrentQueue<long>>> _bufferConcurrent =
        //     new ConcurrentDictionary<Slice, ConcurrentDictionary<Slice, ConcurrentQueue<long>>>(SliceComparer.Instance);

        private readonly Dictionary<Slice, List<long>>[] _buffer;

        private readonly HashSet<long> _entriesToDelete = new();


        private readonly long _postingListContainerId, _entriesContainerId;

        private const string SuggestionsTreePrefix = "__Suggestion_";
        private Dictionary<int, Dictionary<Slice, int>> _suggestionsAccumulator;

#pragma warning disable CS0169
        private Queue<long> _lastEntries; // keep last 256 items
#pragma warning restore CS0169

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index writer with opening semantics and also every new
        // writer becomes essentially a unit of work which makes reusing assets tracking more explicit.

        private IndexWriter(IndexFieldsMapping fieldsMapping)
        {
            _fieldsMapping = fieldsMapping;
            fieldsMapping.UpdateMaximumOutputAndTokenSize();
            _encodingBufferHandler = Analyzer.BufferPool.Rent(fieldsMapping.MaximumOutputSize);
            _tokensBufferHandler = Analyzer.TokensPool.Rent(fieldsMapping.MaximumTokenSize);


            var bufferSize = fieldsMapping!.Count;
            _buffer = new Dictionary<Slice, List<long>>[bufferSize];
            for (int i = 0; i < bufferSize; ++i)
                _buffer[i] = new Dictionary<Slice, List<long>>(SliceComparer.Instance);
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
        }

        public IndexWriter([NotNull] Transaction tx, IndexFieldsMapping fieldsMapping) : this(fieldsMapping)
        {
            Transaction = tx;

            _ownsTransaction = false;
            _postingListContainerId = Transaction.OpenContainer(Constants.IndexWriter.PostingListsSlice);
            _entriesContainerId = Transaction.OpenContainer(Constants.IndexWriter.EntriesContainerSlice);
        }

        public long Index(string id, Span<byte> data)
        {
            using var _ = Slice.From(Transaction.Allocator, id, out var idSlice);
            return Index(idSlice, data);
        }

        public long Index(Slice id, Span<byte> data)
        {
            long entriesCount = Transaction.LowLevelTransaction.RootObjects.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0;
            Transaction.LowLevelTransaction.RootObjects.Add(Constants.IndexWriter.NumberOfEntriesSlice, entriesCount + 1);

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

        public long GetNumberOfEntries()
        {
            return Transaction.LowLevelTransaction.RootObjects.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0L;
        }

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
                    if (entryReader.Read(binding.FieldId, out Span<byte> valueInEntry) == false)
                        break;

                    ExactInsert(valueInEntry);
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
                    if (_encodingBufferHandler.Length < outputSize || _tokensBufferHandler.Length < tokenSize)
                    {
                        UnlikelyGrowBuffer(outputSize, tokenSize);
                    }
                }

                var wordsBuffer = _encodingBufferHandler.AsSpan();
                var tokens = _tokensBufferHandler.AsSpan();
                analyzer.Execute(value, ref wordsBuffer, ref tokens);
                for (int i = 0; i < tokens.Length; i++)
                {
                    ref var token = ref tokens[i];

                    if (token.Offset + token.Length > wordsBuffer.Length)
                        throw new InvalidDataException(
                            $"\nGot token with: \n\tOFFSET {token.Offset}\n\tLENGTH: {token.Length}.\n" +
                            $"Total amount of tokens: {tokens.Length}" +
                            $"\nBuffer contains '{Encodings.Utf8.GetString(wordsBuffer)}' and total length is {wordsBuffer.Length}" +
                            $"\nBuffer from ArrayPool: \n\tbyte buffer is {_encodingBufferHandler.Length} \n\ttokens buffer is {_tokensBufferHandler.Length}" +
                            $"\nOriginal span cointains '{Encodings.Utf8.GetString(value)}' with total length {value.Length}" +
                            $"\nField " +
                            $"\n\tid: {binding.FieldId}" +
                            $"\n\tname: {binding.FieldName}");

                    var word = wordsBuffer.Slice(token.Offset, (int)token.Length);
                    ExactInsert(word);
                }
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

        private unsafe void DeleteCommit(Span<byte> workingBuffer, Tree fieldsTree)
        {
            if (_entriesToDelete.Count == 0)
                return;

            Page lastVisitedPage = default;
            var llt = Transaction.LowLevelTransaction;
            List<long> temporaryStorageForIds = null;

            foreach (var entryToDelete in _entriesToDelete)
            {
                var entryReader = IndexSearcher.GetReaderFor(Transaction, ref lastVisitedPage, entryToDelete);
                foreach (var binding in _fieldsMapping) // todo maciej: this part needs to be rebuilt after implementing DynamicFields
                {
                    if (binding.IsIndexed == false)
                        continue;

                    var fieldType = entryReader.GetFieldType(binding.FieldId, out var intOffset);
                    switch (fieldType)
                    {
                        case IndexEntryFieldType.Empty:
                        case IndexEntryFieldType.Null:
                            var fieldName = fieldType == IndexEntryFieldType.Null ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                            DeleteIdFromExactTerm(binding.FieldId, binding.FieldName, workingBuffer, fieldName.AsReadOnlySpan());
                            break;

                        case IndexEntryFieldType.TupleList:
                            if (entryReader.TryReadMany(binding.FieldId, out var iterator) == false)
                                break;

                            while (iterator.ReadNext())
                            {
                                DeleteIdFromExactTerm(binding.FieldId, binding.FieldName, workingBuffer, iterator.Sequence);
                            }

                            break;

                        case IndexEntryFieldType.Tuple:
                            if (entryReader.Read(binding.FieldId, out Span<byte> valueInEntry) == false)
                                break;
                            DeleteIdFromExactTerm(binding.FieldId, binding.FieldName, workingBuffer, valueInEntry);
                            break;

                        case IndexEntryFieldType.SpatialPointList:
                            if (entryReader.TryReadManySpatialPoint(binding.FieldId, out var spatialIterator) == false)
                                break;

                            while (spatialIterator.ReadNext())
                            {
                                for (int i = 1; i <= spatialIterator.Geohash.Length; ++i)
                                    DeleteIdFromExactTerm(binding.FieldId, binding.FieldName, workingBuffer, spatialIterator.Geohash.Slice(0, i));
                            }

                            break;

                        case IndexEntryFieldType.SpatialPoint:
                            if (entryReader.Read(binding.FieldId, out valueInEntry) == false)
                                break;

                            for (int i = 1; i <= valueInEntry.Length; ++i)
                                DeleteIdFromExactTerm(binding.FieldId, binding.FieldName, workingBuffer, valueInEntry.Slice(0, i));

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
                                    DeleteIdFromExactTerm(binding.FieldId, binding.FieldName, workingBuffer, fieldValue.AsReadOnlySpan());
                                }
                                else
                                {
                                    DeleteIdFromTerm(iterator.Sequence, workingBuffer, binding);
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

                            DeleteIdFromTerm(value, workingBuffer, binding);
                            break;
                    }
                }

                Container.Delete(llt, _entriesContainerId, entryToDelete); // delete raw index entry
                llt.RootObjects.Increment(Constants.IndexWriter.NumberOfEntriesSlice, -1); // update number of entries
                temporaryStorageForIds?.Clear();

                Debug.Assert((llt.RootObjects.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0) >= 0);

                void DeleteIdFromTerm(ReadOnlySpan<byte> termValue, Span<byte> tmpBuf, IndexFieldBinding binding)
                {
                    if (binding.IsAnalyzed == false)
                    {
                        DeleteIdFromExactTerm(entryToDelete, binding.FieldName, tmpBuf, termValue);
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

                    var wordSpace = _encodingBufferHandler.AsSpan();
                    var tokenSpace = _tokensBufferHandler.AsSpan();

                    analyzer.Execute(termValue, ref wordSpace, ref tokenSpace);

                    for (int i = 0; i < tokenSpace.Length; i++)
                    {
                        ref var token = ref tokenSpace[i];

                        var term = wordSpace.Slice(token.Offset, (int)token.Length);
                        DeleteIdFromExactTerm(entryToDelete, binding.FieldName, tmpBuf, term);

                        if (binding.HasSuggestions)
                            RemoveSuggestions(binding, termValue);
                    }
                }


                void DeleteIdFromExactTerm(long id, Slice fieldName, Span<byte> tmpBuffer, ReadOnlySpan<byte> termValue)
                {
                    if (termValue.Length == 0)
                    {
                        Debugger.Launch();
                        Debugger.Break();
                    }

                    // We need to normalize the term in case we have a term bigger than MaxTermLength.
                    using var _ = CreateNormalizedTerm(Transaction.Allocator, termValue, out Slice termSlice);
                    termValue = termSlice.AsReadOnlySpan();

                    var fieldTree = fieldsTree.CompactTreeFor(fieldName);
                    if (termValue.Length == 0 || fieldTree.TryGetValue(termSlice.AsReadOnlySpan(), out var containerId) == false)
                        return;

                    if ((containerId & (long)TermIdMask.Set) != 0)
                    {
                        var setId = containerId & ~0b11;
                        var setStateSpan = Container.GetMutable(llt, setId);
                        ref var setState = ref MemoryMarshal.AsRef<SetState>(setStateSpan);
                        var set = new Set(llt, fieldName, in setState);
                        set.Remove(id);
                        setState = set.State;

                        if (setState.NumberOfEntries == 0)
                        {
                            //If we get rid off all terms we have to remove container. Probably we can do this in a better way
                            fieldTree.TryRemove(termValue, out var __);
                            Container.Delete(llt, _postingListContainerId, setId);
                        }
                    }
                    else if ((containerId & (long)TermIdMask.Small) != 0)
                    {
                        var smallSetId = containerId & ~0b11;
                        var buffer = Container.GetMutable(llt, smallSetId);

                        //Fetch all the ids from the set into temporaryStorageForIds
                        var itemsCount = ZigZagEncoding.Decode<int>(buffer, out var len);
                        temporaryStorageForIds ??= new List<long>(itemsCount);
                        temporaryStorageForIds.Clear();

                        long pos = len;
                        var currentId = 0L;

                        while (pos < buffer.Length)
                        {
                            var delta = ZigZagEncoding.Decode<long>(buffer, out len, (int)pos);
                            pos += len;
                            currentId += delta;
                            if (currentId == id)
                                continue;
                            temporaryStorageForIds.Add(currentId);
                        }

                        // Due to encoding we have to encode new set again so we remove previous small set from container.
                        Container.Delete(llt, _postingListContainerId, smallSetId);
                        fieldTree.TryRemove(termValue, out var __); // term also disappears from the field tree

                        AddNewTerm(temporaryStorageForIds, fieldTree, termValue, tmpBuffer, default, false);
                    }
                    else
                    {
                        fieldTree.TryRemove(termValue, out var _);
                    }
                }
            }
        }

        public bool TryDeleteEntry(string key, string term)
        {
            var fieldsTree = Transaction.ReadTree(Constants.IndexWriter.FieldsSlice);
            if (fieldsTree == null)
                return false;

            var fieldTree = fieldsTree.CompactTreeFor(key);
            var entriesCount = Transaction.LowLevelTransaction.RootObjects.ReadInt64(Constants.IndexWriter.NumberOfEntriesSlice) ?? 0;
            Debug.Assert(entriesCount - _entriesToDelete.Count >= 0);

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
                    _entriesToDelete.Add(iterator.Current);
                }
            }
            else if ((idInTree & (long)TermIdMask.Small) != 0)
            {
                var id = idInTree & ~0b11;
                var smallSet = Container.Get(Transaction.LowLevelTransaction, id).ToSpan();
                // combine with existing value
                var cur = 0L;
                ZigZagEncoding.Decode<int>(smallSet, out var pos); // discard the first entry, the count
                while (pos < smallSet.Length)
                {
                    var value = ZigZagEncoding.Decode<long>(smallSet, out var len, pos);
                    pos += len;
                    cur += value;
                    _entriesToDelete.Add(cur);
                }
            }
            else
            {
                _entriesToDelete.Add(idInTree);
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

            if (_fieldsMapping.Count != 0)
                DeleteCommit(workingBuffer, fieldsTree);

            for (int fieldId = 0; fieldId < _fieldsMapping.Count; ++fieldId)
            {
                var fieldTree = fieldsTree.CompactTreeFor(_fieldsMapping.GetByFieldId(fieldId).FieldName);
                var llt = Transaction.LowLevelTransaction;
                var sortedTerms = _buffer[fieldId].Keys.ToArray();
                Array.Sort(sortedTerms, SliceComparer.Instance);


                foreach (var term in sortedTerms)
                {
                    ReadOnlySpan<byte> termsSpan = term.AsSpan();
                    var entries = _buffer[fieldId][term];
                    if (fieldTree.TryGetValue(termsSpan, out var existing, out var encodedKey) == false)
                    {
                        AddNewTerm(entries, fieldTree, termsSpan, workingBuffer, encodedKey);
                        continue;
                    }

                    if ((existing & (long)TermIdMask.Set) != 0)
                    {
                        var id = existing & ~0b11;
                        var setSpace = Container.GetMutable(llt, id);
                        ref var setState = ref MemoryMarshal.AsRef<SetState>(setSpace);
                        var set = new Set(llt, Slices.Empty, setState);
                        entries.Sort();
                        set.Add(entries);
                        setState = set.State;
                    }
                    else if ((existing & (long)TermIdMask.Small) != 0)
                    {
                        var id = existing & ~0b11;
                        var smallSet = Container.Get(llt, id).ToSpan();
                        // combine with existing value
                        var cur = 0L;
                        ZigZagEncoding.Decode<int>(smallSet, out var pos); // discard the first entry, the count
                        while (pos < smallSet.Length)
                        {
                            var value = ZigZagEncoding.Decode<long>(smallSet, out var len, pos);
                            pos += len;
                            cur += value;
                            entries.Add(cur);
                        }

                        Container.Delete(llt, _postingListContainerId, id);
                        AddNewTerm(entries, fieldTree, termsSpan, workingBuffer, encodedKey);
                    }
                    else // single
                    {
                        // Same element to add, nothing to do here. 
                        if (entries.Count == 1 && entries[0] == existing)
                            continue;

                        entries.Add(existing);
                        AddNewTerm(entries, fieldTree, termsSpan, workingBuffer, encodedKey);
                    }
                }
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

        //Rationale behind passing in the validEncodedKey: because encodedKey could be out of date (due to deletion) so we have to generate it before we add the term.
        //We cannot make EncodedKey nullable either because it is a read-only ref struct
        private unsafe void AddNewTerm(List<long> entries, CompactTree fieldTree, ReadOnlySpan<byte> termsSpan, Span<byte> tmpBuf,
            CompactTree.EncodedKey encodedKey,
            bool validEncodedKey = true)
        {
            if (entries.Count == 0)
                return;

            // common for unique values (guid, date, etc)
            if (entries.Count == 1)
            {
                Debug.Assert(fieldTree.TryGetValue(termsSpan, out var _, out var _) == false);

                // just a single entry, store the value inline
                if (validEncodedKey)
                    fieldTree.Add(termsSpan, entries[0] | (long)TermIdMask.Single, encodedKey);
                else
                    fieldTree.Add(termsSpan, entries[0] | (long)TermIdMask.Single);

                return;
            }

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
                var setId = Container.Allocate(llt, _postingListContainerId, sizeof(SetState), out var setSpace);
                ref var setState = ref MemoryMarshal.AsRef<SetState>(setSpace);
                Set.Create(llt, ref setState);
                var set = new Set(llt, Slices.Empty, setState);
                entries.Sort();
                set.Add(entries);
                setState = set.State;
                if (validEncodedKey)
                    fieldTree.Add(termsSpan, setId | (long)TermIdMask.Set, encodedKey);
                else
                    fieldTree.Add(termsSpan, setId | (long)TermIdMask.Set);
                return;
            }

            var termId = Container.Allocate(llt, _postingListContainerId, pos, out var space);
            tmpBuf.Slice(0, pos).CopyTo(space);
            if (validEncodedKey)
                fieldTree.Add(termsSpan, termId | (long)TermIdMask.Small, encodedKey);
            else
                fieldTree.Add(termsSpan, termId | (long)TermIdMask.Small);
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
        }
    }
}
