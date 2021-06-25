using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Sets;
using Voron.Impl;

namespace Corax
{
    public class IndexWriter : IDisposable // single threaded, controlled by caller
    {
        private readonly StorageEnvironment _environment;
        private readonly TransactionPersistentContext _transactionPersistentContext;

        public readonly Transaction Transaction;        

        private static readonly Slice ContainerIdSlice;

        static IndexWriter()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "ContainerId", ByteStringType.Immutable, out ContainerIdSlice);
            }
        }

        // The reason why we want to have the transaction open for us is so that we avoid having
        // to explicitly provide the index writer with opening semantics and also every new
        // writer becomes essentially a unit of work which makes reusing assets tracking more explicit.
        public IndexWriter([NotNull] StorageEnvironment environment)
        {
            _environment = environment;
            _transactionPersistentContext = new TransactionPersistentContext(true);
            Transaction = _environment.WriteTransaction(_transactionPersistentContext);            

            var exists = Transaction.LowLevelTransaction.RootObjects.Read(ContainerIdSlice);
            if (exists == null)
            {
                _containerId = Container.Create(Transaction.LowLevelTransaction);
                Transaction.LowLevelTransaction.RootObjects.Add(ContainerIdSlice, _containerId);
            }
            else
            {
                _containerId = exists.Reader.ReadLittleEndianInt64();
            }
        }

        private readonly SortedList<Slice, SortedList<Slice, SortedList<long, long>>> _buffer =
            new SortedList<Slice, SortedList<Slice, SortedList<long, long>>>(SliceComparer.Instance);

        private readonly long _containerId;

        public long Index(string id, Span<byte> data, Dictionary<Slice, int> knownFields)
        {
            using var _ = Slice.From(Transaction.Allocator, id, out var idSlice);
            return Index(idSlice, data, knownFields);
        }

        public long Index(Slice id, Span<byte> data, Dictionary<Slice, int> knownFields)
        {
            Span<byte> buf = stackalloc byte[10];
            var idLen = ZigZag.Encode(buf, id.Size);
            var entryId = Container.Allocate(Transaction.LowLevelTransaction, _containerId, idLen + id.Size + data.Length, out var space);
            buf.Slice(0, idLen).CopyTo(space);
            space = space.Slice(idLen);
            id.CopyTo(space);
            space = space.Slice(id.Size);
            data.CopyTo(space);

            var context = Transaction.Allocator;
            var entryReader = new IndexEntryReader(data);
            //entryReader.DebugDump(knownFields);

            foreach (var (key, tokenField) in knownFields)
            {
                if (_buffer.TryGetValue(key, out var field) == false)
                {
                    _buffer[key] = field = new SortedList<Slice, SortedList<long, long>>(SliceComparer.Instance);
                }

                InsertToken(context, ref entryReader, tokenField, field, entryId);
            }

            return entryId;
        }

        private void InsertToken(ByteStringContext context, ref IndexEntryReader entryReader, int tokenField, SortedList<Slice, SortedList<long, long>> field, long entryId)
        {
            var fieldType = entryReader.GetFieldType(tokenField);
            if (fieldType.HasFlag(IndexEntryFieldType.List) && fieldType.HasFlag(IndexEntryFieldType.Tuple))
            {
                var iterator = entryReader.ReadMany(tokenField);
                while (iterator.ReadNext())
                {
                    var value = iterator.Sequence;

                    using var _ = Slice.From(context, value, ByteStringType.Mutable, out var slice);
                    if (field.TryGetValue(slice, out var term) == false)
                    {
                        var fieldName = slice.Clone(context);
                        field[fieldName] = term = new SortedList<long, long>();
                    }
                        
                    term[entryId] = entryId;
                }
            }
            else if (fieldType.HasFlag(IndexEntryFieldType.List))
            {
                var iterator = entryReader.ReadMany(tokenField);
                while (iterator.ReadNext())
                {
                    var value = iterator.Sequence;

                    using var _ = Slice.From(context, value, ByteStringType.Mutable, out var slice);
                    if (field.TryGetValue(slice, out var term) == false)
                    {
                        var fieldName = slice.Clone(context);
                        field[fieldName] = term = new SortedList<long, long>();
                    }

                    term[entryId] = entryId;
                }
            }
            else if (fieldType.HasFlag(IndexEntryFieldType.Tuple))
            {
                entryReader.Read(tokenField, out var value);

                using var _ = Slice.From(context, value, ByteStringType.Mutable, out var slice);
                if (field.TryGetValue(slice, out var term) == false)
                {
                    var fieldName = slice.Clone(context);
                    field[fieldName] = term = new SortedList<long, long>();
                }
                term[entryId] = entryId;
            }
            else if (!fieldType.HasFlag(IndexEntryFieldType.Invalid))
            {
                entryReader.Read(tokenField, out var value);

                using var _ = Slice.From(context, value, ByteStringType.Mutable, out var slice);
                if (field.TryGetValue(slice, out var term) == false)
                {
                    var fieldName = slice.Clone(context);
                    field[fieldName] = term = new SortedList<long, long>();
                }
                term[entryId] = entryId;
            }
            // TODO: Do we want to index nulls? If so, how do we do that?
        }

        public unsafe void Commit()
        {
            using var _ = Transaction.Allocator.Allocate(Container.MaxSizeInsideContainerPage, out Span<byte> tmpBuf);
            Tree fieldsTree = Transaction.CreateTree("Fields");
            foreach (var (field, terms) in _buffer)
            {
                var fieldTree = fieldsTree.CompactTreeFor(field);
                var llt = Transaction.LowLevelTransaction;
                foreach (var (term, entries) in terms)
                {
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
                        set.Add(entries);
                        setState = set.State;
                    }
                    else if ((existing & (long)TermIdMask.Small) != 0)
                    {
                        var id = existing & ~0b11;
                        var smallSet = Container.Get(llt, id);
                        // combine with existing value
                        while (smallSet.IsEmpty == false)
                        {
                            var value = ZigZag.Decode(smallSet, out var len);
                            entries[value] = value;
                            smallSet = smallSet.Slice(len);
                        }
                        Container.Delete(llt, _containerId, id);
                        AddNewTerm(entries, fieldTree, termsSpan, tmpBuf);
                    }
                    else // single
                    {
                        // Same element to add, nothing to do here. 
                        if (entries.Count == 1 && entries.Keys[0] == existing)
                            continue;

                        entries[existing] = existing;
                        AddNewTerm(entries, fieldTree, termsSpan, tmpBuf);
                    }
                }
            }
            Transaction.Commit();
        }

        // container ids are guaranteed to be aligned on 
        // 4 bytes boundary, we're using this to store metadata
        // about the data
        private enum TermIdMask : long
        {
            Single = 0,
            Small = 1,
            Set = 2
        }
        
        private unsafe void AddNewTerm(SortedList<long, long> entries, CompactTree fieldTree, ReadOnlySpan<byte> termsSpan, Span<byte> tmpBuf)
        {
            // common for unique values (guid, date, etc)
            if (entries.Count == 1) 
            {
                Debug.Assert(fieldTree.TryGetValue(termsSpan, out var _) == false);

                // just a single entry, store the value inline
                fieldTree.Add(termsSpan, entries.Keys[0] | (long)TermIdMask.Single);
                return;
            }

            // try to insert to container value
            //TODO: using simplest delta encoding, need to do better here
            int pos = ZigZag.Encode(tmpBuf, entries.Keys[0]);
            var llt = Transaction.LowLevelTransaction;
            for (int i = 1; i < entries.Count; i++)
            {
                if (pos + 10 >= tmpBuf.Length)
                {
                    pos += ZigZag.Encode(tmpBuf.Slice(pos), entries.Keys[i] - entries.Keys[i - 1]);
                    continue;
                }

                // too big, convert to a set
                var setId = Container.Allocate(llt, _containerId, sizeof(SetState), out var setSpace);
                ref var setState = ref MemoryMarshal.AsRef<SetState>(setSpace);
                Set.Initialize(llt, ref setState);
                var set = new Set(llt, Slices.Empty, setState);
                set.Add(entries);
                setState = set.State;
                fieldTree.Add(termsSpan, setId | (long)TermIdMask.Set);
                return;
            }

            var termId = Container.Allocate(llt, _containerId, pos, out var space);
            tmpBuf.Slice(0, pos).CopyTo(space);
            fieldTree.Add(termsSpan, termId | (long)TermIdMask.Small);
        }

        public void Dispose()
        {
            Transaction?.Dispose();
        }
    }
}
