using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Pipeline;
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
        private readonly Analyzer _analyzer;
        private readonly StorageEnvironment _environment;
        private readonly TransactionPersistentContext _transactionPersistentContext;
        private readonly bool _ownsTransaction;
        public readonly Transaction Transaction;

        public static readonly Slice PostingListsSlice, EntriesContainerSlice, FieldsSlice, NumberOfEntriesSlice;

        // CPU bound - embarassingly parallel
        // 
        // private readonly ConcurrentDictionary<Slice, Dictionary<Slice, ConcurrentQueue<long>>> _bufferConcurrent =
        //     new ConcurrentDictionary<Slice, ConcurrentDictionary<Slice, ConcurrentQueue<long>>>(SliceComparer.Instance);

        private readonly Dictionary<Slice, Dictionary<Slice, List<long>>> _buffer =
            new Dictionary<Slice, Dictionary<Slice, List<long>>>(SliceComparer.Instance);

        private readonly List<long> _entriesToDelete = new List<long>();

        private readonly long _postingListContainerId,
            _entriesContainerId;

        private Queue<long> _lastEntries; // keep last 256 items

        static IndexWriter()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Fields", ByteStringType.Immutable, out FieldsSlice);
                Slice.From(ctx, "PostingLists", ByteStringType.Immutable, out PostingListsSlice);
                Slice.From(ctx, "Entries", ByteStringType.Immutable, out EntriesContainerSlice);
                Slice.From(ctx, "NumberOfEntries", ByteStringType.Immutable, out NumberOfEntriesSlice);
            }
        }

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index writer with opening semantics and also every new
        // writer becomes essentially a unit of work which makes reusing assets tracking more explicit.
        public IndexWriter([NotNull] StorageEnvironment environment, Analyzer analyzer = null)
        {
            _environment = environment;
            _transactionPersistentContext = new TransactionPersistentContext(true);
            Transaction = _environment.WriteTransaction(_transactionPersistentContext);
            _ownsTransaction = true;
            _postingListContainerId = Transaction.OpenContainer(PostingListsSlice);
            _entriesContainerId = Transaction.OpenContainer(EntriesContainerSlice);
            _analyzer = analyzer;
        }

        public IndexWriter([NotNull] Transaction tx, Analyzer analyzer = null)
        {
            Transaction = tx;
            _ownsTransaction = false;
            _postingListContainerId = Transaction.OpenContainer(PostingListsSlice);
            _entriesContainerId = Transaction.OpenContainer(EntriesContainerSlice);
            _analyzer = analyzer;
        }

        public long Index(string id, Span<byte> data, Dictionary<Slice, int> knownFields)
        {
            using var _ = Slice.From(Transaction.Allocator, id, out var idSlice);
            return Index(idSlice, data, knownFields);
        }                

        public long Index(Slice id, Span<byte> data, Dictionary<Slice, int> knownFields)
        {
            long entriesCount = Transaction.LowLevelTransaction.RootObjects.ReadInt64(NumberOfEntriesSlice) ?? 0;
            Transaction.LowLevelTransaction.RootObjects.Add(NumberOfEntriesSlice, entriesCount + 1);

            Span<byte> buf = stackalloc byte[10];
            var idLen = ZigZagEncoding.Encode(buf, id.Size);
            var entryId = Container.Allocate(Transaction.LowLevelTransaction, _entriesContainerId, idLen + id.Size + data.Length, out var space);
            buf.Slice(0, idLen).CopyTo(space);
            space = space.Slice(idLen);
            id.CopyTo(space);
            space = space.Slice(id.Size);
            data.CopyTo(space);

            var context = Transaction.Allocator;

            //entryReader.DebugDump(knownFields);

            var entryReader = new IndexEntryReader(data);

            if (_analyzer == null)
            {
                foreach (var (key, tokenField) in knownFields)
                {
                    if (_buffer.TryGetValue(key, out var field) == false)
                    {
                        _buffer[key] = field = new Dictionary<Slice, List<long>>(SliceComparer.Instance);
                    }

                    InsertToken(context, ref entryReader, tokenField, field, entryId);
                }
            }
            else
            {
                foreach (var (key, tokenField) in knownFields)
                {
                    if (_buffer.TryGetValue(key, out var field) == false)
                    {
                        _buffer[key] = field = new Dictionary<Slice, List<long>>(SliceComparer.Instance);
                    }

                    InsertAnalyzedToken(context, ref entryReader, tokenField, field, entryId);
                }
            }

            return entryId;
        }

        public long GetNumberOfEntries()
        {
            return Transaction.LowLevelTransaction.RootObjects.ReadInt64(IndexWriter.NumberOfEntriesSlice) ?? 0;
        }

        [SkipLocalsInit]
        private unsafe void InsertAnalyzedToken(ByteStringContext context, ref IndexEntryReader entryReader, int tokenField, Dictionary<Slice, List<long>> field, long entryId)
        {
            _analyzer.GetOutputBuffersSize(512, out int bufferSize, out int tokenSize);

            byte* tempWordsSpace = stackalloc byte[bufferSize];
            Token* tempTokenSpace = stackalloc Token[tokenSize];

            var fieldType = entryReader.GetFieldType(tokenField);
            if (fieldType.HasFlag(IndexEntryFieldType.List))
            {                                
                // TODO: For performance we can retrieve the whole thing and execute the analyzer many times in a loop for each token
                //       that will ensure faster turnaround and more efficient execution. 
                var iterator = entryReader.ReadMany(tokenField);
                while (iterator.ReadNext())
                {                    
                    // Because of how we store the data, either this is a sequence or a tuple, which also contains a sequence. 
                    var value = iterator.Sequence;

                    var words = new Span<byte>(tempWordsSpace, bufferSize);
                    var tokens = new Span<Token>(tempTokenSpace, tokenSize);
                    _analyzer.Execute(value, ref words, ref tokens);

                    for (int i = 0; i < tokens.Length; i++)
                    {
                        ref var token = ref tokens[i];

                        using var _ = Slice.From(context, words.Slice(token.Offset, (int)token.Length), ByteStringType.Mutable, out var slice);
                        if (field.TryGetValue(slice, out var term) == false)
                        {
                            var fieldName = slice.Clone(context);
                            field[fieldName] = term = new List<long>();
                        }

                        AddMaybeAvoidDuplicate(term, entryId);
                    }
                }
            }           
            else if (fieldType.HasFlag(IndexEntryFieldType.Tuple) || fieldType.HasFlag(IndexEntryFieldType.Invalid) == false)
            {
                entryReader.Read(tokenField, out var value);

                var words = new Span<byte>(tempWordsSpace, bufferSize);
                var tokens = new Span<Token>(tempTokenSpace, tokenSize);
                _analyzer.Execute(value, ref words, ref tokens);

                for (int i = 0; i < tokens.Length; i++)
                {
                    ref var token = ref tokens[i];

                    using var _ = Slice.From(context, words.Slice(token.Offset, (int)token.Length), ByteStringType.Mutable, out var slice);
                    if (field.TryGetValue(slice, out var term) == false)
                    {
                        var fieldName = slice.Clone(context);
                        field[fieldName] = term = new List<long>();
                    }

                    AddMaybeAvoidDuplicate(term, entryId);
                }
            }
        }        
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        void AddMaybeAvoidDuplicate(List<long> term, long entryId)
        {
            // TODO: Do we want to index nulls? If so, how do we do that?
            if (term.Count > 0 && term[^1] == entryId)
                return;

            term.Add(entryId);
        }

        private void InsertToken(ByteStringContext context, ref IndexEntryReader entryReader, int tokenField, Dictionary<Slice, List<long>> field, long entryId)
        {
            var fieldType = entryReader.GetFieldType(tokenField);
            if (fieldType.HasFlag(IndexEntryFieldType.List))
            {
                var iterator = entryReader.ReadMany(tokenField);
                while (iterator.ReadNext())
                {
                    var value = iterator.Sequence;

                    using var _ = Slice.From(context, value, ByteStringType.Mutable, out var slice);
                    if (field.TryGetValue(slice, out var term) == false)
                    {
                        var fieldName = slice.Clone(context);
                        field[fieldName] = term = new List<long>();
                    }

                    AddMaybeAvoidDuplicate(term, entryId);
                }
            }
            else if (fieldType.HasFlag(IndexEntryFieldType.Tuple) || fieldType.HasFlag(IndexEntryFieldType.Invalid) == false)
            {
                entryReader.Read(tokenField, out var value);

                using var _ = Slice.From(context, value, ByteStringType.Mutable, out var slice);
                if (field.TryGetValue(slice, out var term) == false)
                {
                    var fieldName = slice.Clone(context);
                    field[fieldName] = term = new List<long>();
                }

                AddMaybeAvoidDuplicate(term, entryId);
            }
        }

        private unsafe void DeleteCommit(Dictionary<Slice, int> knownFields, Span<byte> tmpBuf, Tree fieldsTree)
        {
            if (_entriesToDelete.Count == 0)
                return;
            
            Page page = default;
            var llt = Transaction.LowLevelTransaction;
            List<long> ids = null;            

            int bufferSize = 0;
            int tokenSize = 0;
            
            var analyzer = _analyzer;
            if ( analyzer != null)
                _analyzer.GetOutputBuffersSize(512, out bufferSize, out tokenSize);
    
            byte* tempWordsSpace = stackalloc byte[bufferSize];
            Token* tempTokenSpace = stackalloc Token[tokenSize];

            foreach (var id in _entriesToDelete)
            {
                var entryReader = IndexSearcher.GetReaderFor(Transaction, ref page, id);

                foreach (var (fieldName, fieldId) in knownFields) // TODO maciej: this is wrong, need to get all the fields from the entry
                {
                    var fieldType = entryReader.GetFieldType(fieldId);
                    if (fieldType.HasFlag(IndexEntryFieldType.List))
                    {
                        var it = entryReader.ReadMany(fieldId);
                        
                        while (it.ReadNext())
                        {
                            if (analyzer == null)
                            {
                                DeleteField(id, fieldName, tmpBuf, it.Sequence);
                            }
                            else
                            {
                                // Because of how we store the data, either this is a sequence or a tuple, which also contains a sequence. 
                                var value = it.Sequence;

                                var words = new Span<byte>(tempWordsSpace, bufferSize);
                                var tokens = new Span<Token>(tempTokenSpace, tokenSize);
                                analyzer.Execute(value, ref words, ref tokens);

                                for (int i = 0; i < tokens.Length; i++)
                                {
                                    ref var token = ref tokens[i];

                                    DeleteField(id, fieldName, tmpBuf, words.Slice(token.Offset, (int)token.Length));
                                }
                            }
                        }
                    }
                    else
                    {
                        entryReader.Read(fieldId, out Span<byte> termValue);
                        if (analyzer == null)
                        {
                            DeleteField(id, fieldName, tmpBuf, termValue);
                        }
                        else
                        {
                            var words = new Span<byte>(tempWordsSpace, bufferSize);
                            var tokens = new Span<Token>(tempTokenSpace, tokenSize);
                            analyzer.Execute(termValue, ref words, ref tokens);

                            for (int i = 0; i < tokens.Length; i++)
                            {
                                ref var token = ref tokens[i];

                                DeleteField(id, fieldName, tmpBuf, words.Slice(token.Offset, (int)token.Length));
                            }
                        }                        
                    }
                }

                ids?.Clear();
                Container.Delete(llt, _entriesContainerId, id);
                llt.RootObjects.Increment(NumberOfEntriesSlice, -1);
                var numberOfEntries = llt.RootObjects.ReadInt64(NumberOfEntriesSlice) ?? 0;
                Debug.Assert(numberOfEntries >= 0);
            }

            void DeleteField(long id, Slice fieldName, Span<byte> tmpBuffer, ReadOnlySpan<byte> termValue)
            {
                var fieldTree = fieldsTree.CompactTreeFor(fieldName);
                //todo maciej: got some issue here after refactoring
                if (fieldTree.TryGetValue(termValue, out var containerId) == false)
                    return;

                if ((containerId & (long)TermIdMask.Set) != 0)
                {
                    var setId = containerId & ~0b11;
                    var setStateSpan = Container.GetMutable(llt, setId);
                    ref var setState = ref MemoryMarshal.AsRef<SetState>(setStateSpan);
                    var set = new Set(llt, fieldName, in setState);
                    set.Remove(id);
                    setState = set.State;
                }
                else if ((containerId & (long)TermIdMask.Small) != 0)
                {
                    var smallSetId = containerId & ~0b11;
                    var buffer = Container.GetMutable(llt, smallSetId);

                    //get first item into ids.
                    var itemsCount = ZigZagEncoding.Decode<int>(buffer, out var len);
                    ids ??= new List<long>(itemsCount);
                    ids.Clear();


                    long pos = len;
                    var currentId = 0L;

                    while (pos < buffer.Length)
                    {
                        var delta = ZigZagEncoding.Decode<long>(buffer, out len, (int)pos);
                        pos += len;
                        currentId += delta;
                        if (currentId == id)
                            continue;
                        ids.Add(currentId);
                    }

                    Container.Delete(llt, _postingListContainerId, smallSetId);
                    AddNewTerm(ids, fieldTree, termValue, tmpBuffer);
                }
                else
                {
                    fieldTree.TryRemove(termValue, out var _);
                }
            }
        }

        public bool TryDeleteEntry(string key, string term)
        {
            var fieldsTree = Transaction.ReadTree(FieldsSlice);
            var fieldTree = fieldsTree.CompactTreeFor(key);
            var entriesCount = Transaction.LowLevelTransaction.RootObjects.ReadInt64(NumberOfEntriesSlice) ?? 0;
            Debug.Assert(entriesCount - _entriesToDelete.Count >= 1);
            
            if (fieldTree.TryGetValue(term, out long id) == false)
                return false;

            if ((id & (long)TermIdMask.Set) != 0 || (id & (long)TermIdMask.Small) != 0)
                throw new InvalidDataException($"Cannot delete {term} in {key} because it's not {nameof(TermIdMask.Single)}.");
                    
            if (fieldTree.TryRemove(term, out id) == false)
                return false;

            _entriesToDelete.Add(id);
            return true;
        }

        public void Commit(Dictionary<Slice, int> knownFields = null)
        {
            using var _ = Transaction.Allocator.Allocate(Container.MaxSizeInsideContainerPage, out Span<byte> tmpBuf);
            Tree fieldsTree = Transaction.CreateTree(FieldsSlice);
            
            if(knownFields != null)
                DeleteCommit(knownFields, tmpBuf, fieldsTree);
            
            foreach (var (field, terms) in _buffer)
            {
                var fieldTree = fieldsTree.CompactTreeFor(field);
                var llt = Transaction.LowLevelTransaction;
                var sortedTerms = terms.Keys.ToArray();
                // CPU bounded - embarssingly parallel
                Array.Sort(sortedTerms, SliceComparer.Instance);
                foreach (var term in sortedTerms)
                {
                    var entries = terms[term];
                    ReadOnlySpan<byte> termsSpan = term.AsSpan();

                    // TODO: For now if the term is null (termsSpan.Length == 0) we will not do anything... this happens
                    //       because we are not explicitly handling the case of explicit NULL values (instead of unsetted). 
                    if (termsSpan.Length == 0)
                        continue;

                    if (fieldTree.TryGetValue(termsSpan, out var existing) == false)
                    {
                        AddNewTerm(entries, fieldTree, termsSpan, tmpBuf);
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
                        AddNewTerm(entries, fieldTree, termsSpan, tmpBuf);
                    }
                    else // single
                    {
                        // Same element to add, nothing to do here. 
                        if (entries.Count == 1 && entries[0] == existing)
                            continue;

                        entries.Add(existing);
                        AddNewTerm(entries, fieldTree, termsSpan, tmpBuf);
                    }
                }
            }

            if (_ownsTransaction)
                Transaction.Commit();
        }

        private unsafe void AddNewTerm(List<long> entries, CompactTree fieldTree, ReadOnlySpan<byte> termsSpan, Span<byte> tmpBuf)
        {
            // common for unique values (guid, date, etc)
            if (entries.Count == 1)
            {
                Debug.Assert(fieldTree.TryGetValue(termsSpan, out var _) == false);

                // just a single entry, store the value inline
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
                Set.Initialize(llt, ref setState);
                var set = new Set(llt, Slices.Empty, setState);
                entries.Sort();
                set.Add(entries);
                setState = set.State;
                fieldTree.Add(termsSpan, setId | (long)TermIdMask.Set);
                return;
            }

            var termId = Container.Allocate(llt, _postingListContainerId, pos, out var space);
            tmpBuf.Slice(0, pos).CopyTo(space);
            fieldTree.Add(termsSpan, termId | (long)TermIdMask.Small);
        }

        public void Dispose()
        {
            if (_ownsTransaction)
                Transaction?.Dispose();
        }
    }
}
