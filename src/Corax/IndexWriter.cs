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
using System.Xml;
using Corax.Pipeline;
using Corax.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Compression;
using Voron;
using Voron.Data;
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

        private struct EntriesModifications
        {
            public List<long> Additions;
            public List<long> Removals;
        }
        
        private readonly Dictionary<Slice, EntriesModifications>[] _buffer;
        private readonly Dictionary<long, EntriesModifications>[] _bufferLongs;
        private readonly Dictionary<double, EntriesModifications>[] _bufferDoubles;
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
                        Debug.Assert((fieldType & IndexEntryFieldType.Tuple) == 0);
                        
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
                        $"{Environment.NewLine}Original span cointains '{Encodings.Utf8.GetString(value)}' with total length {value.Length}" +
                        $"{Environment.NewLine}Field " +
                        $"{Environment.NewLine}\tid: {binding.FieldId}" +
                        $"{Environment.NewLine}\tname: {binding.FieldName}");
                }
            }

            void NumericInsert(long lVal, double dVal)
            {
                if (fieldDoubles.TryGetValue(dVal, out var doublesTerms) == false)
                {
                    fieldDoubles[dVal] = doublesTerms = new EntriesModifications { Additions = new List<long>(), Removals = new List<long>() };
                }

                if (fieldLongs.TryGetValue(lVal, out var longsTerms) == false)
                {
                    fieldLongs[lVal] = longsTerms = new EntriesModifications { Additions = new List<long>(), Removals = new List<long>() };
                }

                AddMaybeAvoidDuplicate(doublesTerms.Additions, entryId);
                AddMaybeAvoidDuplicate(longsTerms.Additions, entryId);
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
                    field[fieldName] = term = new EntriesModifications { Additions = new List<long>(), Removals = new List<long>() };
                }

                AddMaybeAvoidDuplicate(term.Additions, entryId);

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

        private void RecordTermsToDeleteFrom(long entryToDelete,  LowLevelTransaction llt, ref Page lastVisitedPage)
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
                        var termValue = fieldType == IndexEntryFieldType.Null ? Constants.NullValueSlice : Constants.EmptyStringSlice;
                        RecordExactTermToDelete(termValue, binding);
                        break;
                    case IndexEntryFieldType.TupleListWithNulls:
                    case IndexEntryFieldType.TupleList:
                    {
                        if (entryReader.TryReadMany(binding.FieldId, out var iterator) == false)
                            break;

                        while (iterator.ReadNext())
                        {
                            if (iterator.IsNull)
                            {
                                RecordTupleToDelete(binding, Constants.NullValueSlice, double.NaN, 0);
                            }
                            else if (iterator.IsEmpty)
                            {
                                throw new InvalidDataException("Tuple list cannot contain an empty string (otherwise, where did the numeric came from!)");
                            }
                            else
                            {
                                RecordTupleToDelete(binding, iterator.Sequence, iterator.Double, iterator.Long);
                            }
                        }
                        break;
                    }
                    case IndexEntryFieldType.Tuple:
                        if (entryReader.Read(binding.FieldId, out _, out long l, out double d, out Span<byte> valueInEntry) == false)
                            break;
                        RecordTupleToDelete(binding, valueInEntry, d, l);
                        break;

                    case IndexEntryFieldType.SpatialPointList:
                        if (entryReader.TryReadManySpatialPoint(binding.FieldId, out var spatialIterator) == false)
                            break;

                        while (spatialIterator.ReadNext())
                        {
                            for (int i = 1; i <= spatialIterator.Geohash.Length; ++i)
                            {
                                var spatialTerm = spatialIterator.Geohash.Slice(0, i);
                                RecordExactTermToDelete(spatialTerm, binding);
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
                        if (entryReader.TryReadMany(binding.FieldId, out var iterator) == false)
                            break;

                        while (iterator.ReadNext())
                        {
                            if (iterator.IsNull)
                            {
                                RecordExactTermToDelete(Constants.NullValueSlice, binding);
                            }
                            else if (iterator.IsEmpty)
                            {
                                RecordExactTermToDelete(Constants.EmptyStringSlice, binding);
                            }
                            else
                            {
                                RecordTermToDelete(iterator.Sequence, binding); 
                            }
                        }
                        break;
                    }

                    case IndexEntryFieldType.SpatialPoint:
                        if (entryReader.Read(binding.FieldId, out valueInEntry) == false)
                            break;

                        for (int i = 1; i <= valueInEntry.Length; ++i)
                        {
                            var spatialTerm = valueInEntry.Slice(0, i);
                            RecordExactTermToDelete(spatialTerm, binding);
                        }
                        break;
                    default:
                        if (entryReader.Read(binding.FieldId, out var value) == false)
                            break;
                        
                        if(value.IsEmpty)
                            goto case IndexEntryFieldType.Empty;

                        RecordTermToDelete(value, binding);
                        break;
                }
            }

            Container.Delete(llt, _entriesContainerId, entryToDelete); // delete raw index entry

            void RecordTupleToDelete(IndexFieldBinding binding, ReadOnlySpan<byte> termValue, double termDouble, long termLong)
            {
                // Is there any reason to analyze string of number?
                RecordExactTermToDelete(termValue, binding);

                if (_bufferDoubles[binding.FieldId].TryGetValue(termDouble, out var result) == false)
                {
                    _bufferDoubles[binding.FieldId][termDouble] = result = new EntriesModifications { Additions = new List<long>(), Removals = new List<long>() };
                }

                AddMaybeAvoidDuplicate(result.Removals, entryToDelete);

                if (_bufferLongs[binding.FieldId].TryGetValue(termLong, out result) == false)
                {
                    _bufferLongs[binding.FieldId][termLong] = result = new EntriesModifications { Additions = new List<long>(), Removals = new List<long>() };
                }
                AddMaybeAvoidDuplicate(result.Removals, entryToDelete);
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
            
            void RecordExactTermToDelete( ReadOnlySpan<byte> termValue, IndexFieldBinding binding)
            {
                using var _ = CreateNormalizedTerm(Transaction.Allocator, termValue, out Slice termSlice);

                if (_buffer[binding.FieldId].TryGetValue(termSlice, out var result) == false)
                {
                    _buffer[binding.FieldId][termSlice.Clone(Transaction.Allocator)] =
                        result = new EntriesModifications { Additions = new List<long>(), Removals = new List<long>() };
                }
                
                AddMaybeAvoidDuplicate(result.Removals, entryToDelete);
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
            var llt = Transaction.LowLevelTransaction;
            var currentFieldTerms = _buffer[fieldId];
            int termsCount = currentFieldTerms.Count;
            if (sortedTermsBuffer.Length < termsCount)
            {
                if (sortedTermsBuffer.Length > 0)
                    ArrayPool<Slice>.Shared.Return(sortedTermsBuffer);
                sortedTermsBuffer = ArrayPool<Slice>.Shared.Rent(termsCount);
            }
            currentFieldTerms.Keys.CopyTo(sortedTermsBuffer, 0);
            Array.Sort(sortedTermsBuffer,0, termsCount, SliceComparer.Instance);


            fieldTree.InitializeStateForTryGetNextValue();
            for (var index = 0; index < termsCount; index++)
            {
                var term = sortedTermsBuffer[index];
                var entries = currentFieldTerms[term];

                long termId;
                ReadOnlySpan<byte> termsSpan = term.AsSpan();
                if (fieldTree.TryGetValue(termsSpan, out var existing, out var encodedKey) == false)
                {
                    Debug.Assert(entries.Removals.Count == 0);
                    AddNewTerm(entries.Additions, tmpBuf, out termId);
                    fieldTree.Add(termsSpan, termId, encodedKey);
                    continue;
                }

                switch (AddEntriesToTerm(tmpBuf, existing, llt, entries, out termId))
                {
                    case AddEntriesToTermResult.UpdateTermId:
                        fieldTree.Add(termsSpan, termId, encodedKey);
                        break;
                    case AddEntriesToTermResult.RemoveTermId:
                        fieldTree.TryRemove(termsSpan, out _);
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

        private AddEntriesToTermResult AddEntriesToTerm(Span<byte> tmpBuf, long existing, LowLevelTransaction llt, in EntriesModifications entries, out long termId)
        {
            if ((existing & (long)TermIdMask.Set) != 0)
            {
                return AddEntriesToTermResultViaLargeSet(llt, entries, out termId, existing & Constants.StorageMask.ContainerType);
            }
            if ((existing & (long)TermIdMask.Small) != 0)
            {
                return AddEntriesToTermResultViaSmallSet(tmpBuf, llt, entries, out termId, existing & Constants.StorageMask.ContainerType);
            }
            return AddEntriesToTermResultSingleValue(tmpBuf, existing, entries, out termId);
        }

        private AddEntriesToTermResult AddEntriesToTermResultViaSmallSet(Span<byte> tmpBuf, LowLevelTransaction llt, EntriesModifications entries, out long termId, long id)
        {
            var smallSet = Container.GetMutable(llt, id);
            Debug.Assert(entries.Removals.Distinct().Count() == entries.Removals.Count, $"Removals list is not distinct.");
            
            entries.Removals.Sort();
          
            int removalIndex = 0;
            
            // combine with existing values
            var currentId = 0L;
            var count = ZigZagEncoding.Decode<int>(smallSet, out var positionInEncodedBuffer);

            for (int idX = 0; idX < count; ++idX)
            {
                var value = ZigZagEncoding.Decode<long>(smallSet, out var lengthOfDelta, positionInEncodedBuffer);
                positionInEncodedBuffer += lengthOfDelta;
                currentId += value;
                
                if (removalIndex < entries.Removals.Count)
                {
                    if (currentId == entries.Removals[removalIndex])
                    {
                        removalIndex++;
                        continue;
                    }

                    if (currentId > entries.Removals[removalIndex])
                        throw new InvalidDataException("Attempt to remove value " + entries.Removals[removalIndex] + ", but got " + currentId);
                }
                
                AddMaybeAvoidDuplicate(entries.Additions, currentId);
            }

            if (entries.Additions.Count == 0)
            {
                Container.Delete(llt, _postingListContainerId, id);
                termId = -1;
                return AddEntriesToTermResult.RemoveTermId;
            }

            entries.Additions.Sort();

            if (TryDeltaEncodingToBuffer(entries.Additions, tmpBuf, out var encoded) == false)
            {
                AddNewTermToSet(entries.Additions, out termId);;
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

        private AddEntriesToTermResult AddEntriesToTermResultSingleValue(Span<byte> tmpBuf, long existing, EntriesModifications entries, out long termId)
        {
            // single
            if (entries.Additions.Count == 1 && entries.Additions[0] == existing && entries.Removals.Count == 0)
            {
                // Same element to add, nothing to do here.
                termId = -1;
                return AddEntriesToTermResult.NothingToDo;
            }

            if (entries.Removals.Count != 0)
            {
                if (entries.Removals[0] != existing || entries.Removals.Count != 1)
                    throw new InvalidDataException($"Attempt to delete id {string.Join(", ", entries.Removals)} that does not exists, only value is: {existing}");

                if (entries.Additions.Count == 0)
                {
                    termId = -1;
                    return AddEntriesToTermResult.RemoveTermId;
                }
            }
            else
            {
                AddMaybeAvoidDuplicate(entries.Additions, existing);
            }
            
            AddNewTerm(entries.Additions, tmpBuf, out termId);
            return AddEntriesToTermResult.UpdateTermId;
        }

        private AddEntriesToTermResult AddEntriesToTermResultViaLargeSet(LowLevelTransaction llt, EntriesModifications entries, out long termId, long id)
        {
            var setSpace = Container.GetMutable(llt, id);
            ref var setState = ref MemoryMarshal.AsRef<SetState>(setSpace);
            var set = new Set(llt, Slices.Empty, setState);
            entries.Additions.Sort();
            entries.Removals.Sort();
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
            var llt = Transaction.LowLevelTransaction;
          
            foreach (var (term, entries) in _bufferLongs[fieldId])
            {
                long termId;

                using var _ = fieldTree.Read(term, out var result);
                if (result.HasValue == false)
                {
                    Debug.Assert(entries.Removals.Count == 0);
                    AddNewTerm(entries.Additions, tmpBuf, out termId);
                    fieldTree.Add(term, termId);
                    continue;
                }

                var existing = *((long*)result.Content.Ptr);
                switch (AddEntriesToTerm(tmpBuf, existing, llt, entries, out termId))
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
            var llt = Transaction.LowLevelTransaction;

            foreach (var (term, entries) in _bufferDoubles[fieldId])
            {
                using var _ = fieldTree.Read(term, out var result);

                long termId;
                if (result.Size == 0) // no existing value
                {
                    Debug.Assert(entries.Removals.Count == 0);
                    AddNewTerm(entries.Additions, tmpBuf, out termId);
                    fieldTree.Add(term, termId);
                    continue;
                }

                var existing = *((long*)result.Content.Ptr);
                switch (AddEntriesToTerm(tmpBuf, existing, llt, entries, out termId))
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
        
        private bool TryDeltaEncodingToBuffer(List<long> additions, Span<byte> tmpBuf, out Span<byte> encoded)
        {
            // try to insert to container value
            //TODO: using simplest delta encoding, need to do better here
            int pos = ZigZagEncoding.Encode(tmpBuf, additions.Count);
            pos += ZigZagEncoding.Encode(tmpBuf, additions[0], pos);
            for (int i = 1; i < additions.Count; i++)
            {
                if (pos + ZigZagEncoding.MaxEncodedSize >= tmpBuf.Length)
                {
                    encoded = default;
                    return false;
                }

                long entry = additions[i] - additions[i - 1];
                if (entry == 0)
                    continue; // we don't need to store duplicates

                pos += ZigZagEncoding.Encode(tmpBuf, entry, pos);
            }

            encoded = tmpBuf[..pos];
            return true;
        }

        private void AddNewTerm(List<long> additions, Span<byte> tmpBuf, out long termId, bool sortingNeeded = true)
        {
            Debug.Assert(additions.Count > 0);
            // common for unique values (guid, date, etc)
            if (additions.Count == 1)
            {
                termId = additions[0] | (long)TermIdMask.Single;                
                return;
            }

            if(sortingNeeded)
                additions.Sort();

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

        private unsafe void AddNewTermToSet(List<long> additions, out long termId)
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
