using Sparrow;
using Sparrow.Binary;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Voron.Exceptions;
using Voron.Impl;

namespace Voron.Data.Compact
{
    unsafe partial class PrefixTree
    {
        [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 16)]
        internal unsafe struct Entry
        {
            [FieldOffset(0)]
            public uint Hash;

            [FieldOffset(4)]
            public uint Signature;

            [FieldOffset(8)]
            public long NodePtr;

            public Entry(uint hash, uint signature, long nodePtr)
            {
                this.Hash = hash;
                this.Signature = signature;
                this.NodePtr = nodePtr;
            }
        }

        internal sealed class InternalTable
        {
            private const int InitialCapacity = 64;

            private const uint kDeleted = 0xFFFFFFFE;
            private const uint kUnused = 0xFFFFFFFF;
            private const long kInvalidNode = -1;

            private const uint kHashMask = 0xFFFFFFFE;
            private const uint kSignatureMask = 0x7FFFFFFE;
            private const uint kDuplicatedMask = 0x80000000;

            /// <summary>
            /// By default, if you don't specify a hashtable size at construction-time, we use this size.  Must be a power of two, and at least kMinCapacity.
            /// </summary>
            private const int kInitialCapacity = 64;

            /// <summary>
            /// By default, if you don't specify a hashtable size at construction-time, we use this size.  Must be a power of two, and at least kMinCapacity.
            /// </summary>
            private const int kMinCapacity = 4;

            /// <summary>
            /// LoadFactor - controls hash map load. 4 means 100% load, ie. hashmap will grow
            /// when number of items == capacity. Default value of 6 means it grows when
            /// number of items == capacity * 3/2 (6/4). Higher load == tighter maps, but bigger
            /// risk of collisions.
            /// </summary>
            public const int LoadFactor = 6;

            private readonly PrefixTree owner;
            private readonly LowLevelTransaction _tx;
            private readonly PrefixTreeRootMutableState _root;
            private readonly int _entriesPerPage;
            private PrefixTreeTablePageMutableState _state;

            /// <summary>
            /// The current capacity of the dictionary
            /// </summary>
            public int Capacity
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return this._state.Capacity; }
            }

            /// <summary>
            /// This is the real counter of how many items are in the hash-table (regardless of buckets)
            /// </summary>
            public int Count
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return this._state.Size; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { this._state.Size = value; }
            }

            /// <summary>
            /// How many used buckets. 
            /// </summary>
            private int NumberOfUsed
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return this._state.NumberOfUsed; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { this._state.NumberOfUsed = value; }
            }

            /// <summary>
            /// How many occupied buckets are marked deleted
            /// </summary>
            private int NumberOfDeleted
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return this._state.NumberOfDeleted; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { this._state.NumberOfDeleted = value; }
            }

            /// <summary>
            /// The next growth threshold. 
            /// </summary>
            private int NextGrowthThreshold
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return this._state.NextGrowthThreshold; }
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set { this._state.NextGrowthThreshold = value; }
            }

            public InternalTable(PrefixTree owner, LowLevelTransaction tx, PrefixTreeRootMutableState root)
            {
                this.owner = owner;

                this._tx = tx;
                this._root = root;
                this._entriesPerPage = _tx.DataPager.PageMaxSpace / sizeof(Entry);

                var page = tx.GetPage(root.Table);
                this._state = new PrefixTreeTablePageMutableState(tx, page);                
            }      
            

            private static int GetEntriesPerPage(LowLevelTransaction tx)
            {
                return tx.DataPager.PageMaxSpace / sizeof(Entry);
            }

            private static int GetPagesToAllocate(int entriesPerPage, int capacity)
            {
                // This is the amount of pages required to allocate the whole table in contiguous disk space.                               
                return (capacity / entriesPerPage) + 2;
            }      

            internal static Page Allocate(LowLevelTransaction tx, PrefixTreeRootMutableState root, int newCapacity = InitialCapacity)
            {
                // Calculate the next power of 2.
                newCapacity = Bits.NextPowerOf2(newCapacity);

                int entriesPerPage = GetEntriesPerPage(tx);
                var pagesCount = GetPagesToAllocate(entriesPerPage, newCapacity);

                var page = tx.AllocatePage(pagesCount);
                tx.BreakLargeAllocationToSeparatePages(page.PageNumber);

                var tableHeader = (PrefixTreeTablePageHeader*)page.Pointer;
                tableHeader->Flags = tableHeader->Flags | PageFlags.ZFastTreePage;
                tableHeader->Capacity = newCapacity;
                tableHeader->NumberOfUsed = 0;
                tableHeader->NumberOfDeleted = 0;
                tableHeader->Size = 0;
                tableHeader->NextGrowthThreshold = newCapacity * 4 / LoadFactor;

                // Initialize the whole memory block with the initial values. 
                var firstEntriesPage = tx.ModifyPage(page.PageNumber + 1);
                firstEntriesPage.Flags |= PageFlags.ZFastTreePage;

                byte* srcPtr = firstEntriesPage.DataPointer;
                BlockCopyMemoryHelper.Memset((Entry*)srcPtr, entriesPerPage, new Entry(kUnused, kUnused, kInvalidNode));

                // Initialize using a copy trick. Because we are using 4K pages,
                // the source page will usually be L1 cached improving performance.
                int length = tx.DataPager.PageMaxSpace;
                for ( int i = 2; i < pagesCount; i++ )
                {
                    var dataPage = tx.ModifyPage(page.PageNumber + i);
                    dataPage.Flags |= PageFlags.ZFastTreePage;
                    byte* destPtr = dataPage.DataPointer;

                    Memory.Copy(destPtr, srcPtr, length);
                }
                
                return page;
            }

            public void Add(long nodePtr, uint rHash)
            {
                ResizeIfNeeded();

                // We shrink the signature to the proper size (30 bits)
                uint signature = rHash & kSignatureMask;
                uint hash = rHash & kHashMask;
                int bucket = (int)(hash % Capacity);

                int numProbes = 1;
                do
                {
                    var entry = ReadEntry(bucket);
                    if (entry->Signature == signature)
                    {
                        entry->Signature |= kDuplicatedMask;
                        WriteEntry(bucket, entry->Hash, entry->Signature, entry->NodePtr);
                    }                        

                    uint nHash = entry->Hash;
                    if (nHash == kUnused)
                    {
                        this._state.NumberOfUsed++;

                        goto SET;
                    }
                    else if (nHash == kDeleted)
                    {
                        this._state.NumberOfDeleted--;
                        goto SET;
                    }

                    bucket = (bucket + numProbes) % Capacity;
                    numProbes++;
                }
                while (true);

                SET:
                this._state.Size++;
                WriteEntry(bucket, hash, signature, nodePtr);

#if DETAILED_DEBUG_H
                Console.WriteLine(string.Format("Add: {0}, Bucket: {1}, Signature: {2}", node.ToDebugString(this.owner), bucket, signature));
#endif
#if VERIFY
                VerifyStructure();
#endif
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private Entry* ReadEntry(int bucket)
            {
                return ReadEntry(bucket, this._root.Table);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private Entry* ReadEntry(int bucket, long tablePage)
            {
                int pageNumber = bucket / _entriesPerPage;
                int entryNumber = bucket % _entriesPerPage;

                // TODO: Cache the last page (it will be probably be a hit).

                Page page = _tx.GetPage(tablePage + pageNumber + 1);
                return ((Entry*)page.DataPointer) + entryNumber;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteEntry(int bucket, uint uhash, uint signature, long nodePtr)
            {
                WriteEntry(bucket, this._root.Table, uhash, signature, nodePtr);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void WriteEntry(int bucket, long tablePage, uint uhash, uint signature, long nodePtr)
            {
                int pageNumber = bucket / _entriesPerPage;
                int entryNumber = bucket % _entriesPerPage;

                // TODO: Cache the last page (it will be probably be a hit).

                Page page = _tx.ModifyPage(tablePage + pageNumber + 1);

                var entry = ((Entry*)page.DataPointer) + entryNumber;
                entry->Hash = uhash;
                entry->Signature = signature;
                entry->NodePtr = nodePtr;
            }

            public void Remove(long nodePtr, uint rHash)
            {
                // We shrink the signature to the proper size (30 bits)
                uint signature = rHash & kSignatureMask;
                uint hash = rHash & kHashMask;
                int bucket = (int)(hash % Capacity);

                int lastDuplicated = -1;
                uint numProbes = 1; // how many times we've probed

                var entry = ReadEntry(bucket);
                do
                {
                    if ((entry->Signature & kSignatureMask) == signature)
                        lastDuplicated = bucket;

                    if (entry->NodePtr == nodePtr)
                    {
                        // This is the last element and is not a duplicate, therefore the last one is not a duplicate anymore. 
                        if ((entry->Signature & kDuplicatedMask) == 0 && lastDuplicated != -1)
                        { 
                            entry->Signature &= kSignatureMask;
                            WriteEntry(bucket, entry->Hash, entry->Signature, entry->NodePtr);
                        }
                            
                        if (entry->Hash < kDeleted)
                        {
#if DETAILED_DEBUG_H
                            Console.WriteLine(string.Format("Remove: {0}, Bucket: {1}, Signature: {2}", node.ToDebugString(this.owner), bucket, signature));
#endif
                            WriteEntry(bucket, kDeleted, kUnused, kInvalidNode);

                            this._state.NumberOfDeleted++;
                            this._state.Size--;
                        }

                        if (3 * this.NumberOfDeleted / 2 > this.Capacity - this.NumberOfUsed)
                        {
                            // We will force a rehash with the growth factor based on the current size.
                            Shrink(Math.Max(InitialCapacity, this._state.Size * 2));
                        }

                        return;
                    }

                    bucket = (int)((bucket + numProbes) % Capacity);
                    numProbes++;                    

                    Debug.Assert(numProbes < 100);

                    entry = ReadEntry(bucket);
                }
                while (entry->Hash != kUnused);
            }

            public void Replace(long oldNodePtr, long newNodePtr, uint rHash)
            {
                // We shrink the signature to the proper size (30 bits)
                uint signature = rHash & kSignatureMask;
                uint hash = rHash & kHashMask;
                int bucket = (int)(hash % Capacity);

                int numProbes = 1;

                var entry = ReadEntry(bucket);
                while (entry->NodePtr != oldNodePtr)
                {
                    bucket = (bucket + numProbes) % Capacity;
                    numProbes++;

                    entry = ReadEntry(bucket);
                }

                AssertReplace(bucket, (int)hash, newNodePtr);

                WriteEntry(bucket, entry->Hash, entry->Signature, newNodePtr);

#if DETAILED_DEBUG_H
                Console.WriteLine(string.Format("Old: {0}, Bucket: {1}, Signature: {2}", oldNode.ToDebugString(this.owner), pos, hash, signature));
                Console.WriteLine(string.Format("New: {0}", newNode.ToDebugString(this.owner)));
#endif

#if VERIFY
                VerifyStructure();
#endif

            }

            [Conditional("DEBUG")]
            public void AssertReplace(int pos, int hash, long newNodePtr)
            {
                var entry = ReadEntry(pos);

                Debug.Assert(entry->NodePtr != kInvalidNode);
                Debug.Assert(entry->Hash == (uint)hash);

                var node = this.owner.ReadNodeByName(entry->NodePtr);
                var newNode = this.owner.ReadNodeByName(newNodePtr);
                Debug.Assert(this.owner.Handle(node).CompareTo(this.owner.Handle(newNode)) == 0);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int GetExactPosition(BitVector key, int prefixLength, uint rHash)
            {
                // We shrink the signature to the proper size (30 bits)
                uint signature = rHash & kSignatureMask;
                uint hash = rHash & kHashMask;
                int bucket = (int)(hash % Capacity);

                int numProbes = 1;

                var entry = ReadEntry(bucket);

                uint nSignature;
                do
                {
                    nSignature = entry->Signature;

                    if ((nSignature & kSignatureMask) == signature)
                    {
                        Node* node = this.owner.ReadNodeByName(entry->NodePtr);
                        if (this.owner.GetHandleLength(node) == prefixLength)
                        {
                            Node* referenceNodePtr = this.owner.ReadNodeByName(node->ReferencePtr);
                            if ( key.IsPrefix(this.owner.Name(referenceNodePtr), prefixLength))
                                return bucket;
                        }                            
                    }

                    bucket = (bucket + numProbes) % Capacity;
                    numProbes++;

                    Debug.Assert(numProbes < 100);

                    entry = ReadEntry(bucket);
                }
                while (entry->Hash != kUnused);

                return -1;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int GetPosition(BitVector key, int prefixLength, uint rHash)
            {
                // We shrink the signature to the proper size (30 bits)
                uint signature = rHash & kSignatureMask;
                uint hash = rHash & kHashMask;
                int bucket = (int)(hash % Capacity);

                int numProbes = 1;

                var entry = ReadEntry(bucket);

                uint nSignature;
                do
                {
                    nSignature = entry->Signature;

                    if ((nSignature & kSignatureMask) == signature)
                    {                        
                        var node = (Node*)this.owner.ReadNodeByName(entry->NodePtr);
                        if ((nSignature & kDuplicatedMask) == 0) 
                            return bucket;
                        
                        if (this.owner.GetHandleLength(node) == prefixLength)
                        {
                            Node* referenceNodePtr = this.owner.ReadNodeByName(node->ReferencePtr);
                            if (key.IsPrefix(this.owner.Name(referenceNodePtr), prefixLength))
                                return bucket;
                        }

                    }

                    bucket = (bucket + numProbes) % Capacity;
                    numProbes++;

                    Debug.Assert(numProbes < 100);

                    entry = ReadEntry(bucket);
                }
                while (entry->Hash != kUnused);

                return -1;
            }

            public long this[int position]
            {
                get
                {
                    return ReadEntry(position)->NodePtr;
                }
            }

            internal string DumpNodesTable(PrefixTree tree)
            {
                var builder = new StringBuilder();

                bool first = true;
                builder.Append("After Insertion. NodesTable: {");
                foreach (var node in this.Values)
                {
                    if (!first)
                        builder.Append(", ");
                    
                    var copyOfNode = this.owner.ReadNodeByName( node );

                    builder.Append(tree.Handle(copyOfNode).ToDebugString())
                           .Append(" => ")
                           .Append(tree.ToDebugString(copyOfNode));

                    first = false;
                }
                builder.Append("} Root: ")
                       .Append(tree.ToDebugString( tree.Root ));

                return builder.ToString();
            }

            internal string DumpTable()
            {
                var builder = new StringBuilder();

                builder.AppendLine("NodesTable: {");

                for (int i = 0; i < this.Capacity; i++)
                {
                    var entry = ReadEntry(i);
                    if (entry->Hash != kUnused && entry->Hash != kDeleted)
                    {
                        var node = this.owner.ReadNodeByName(entry->NodePtr);

                        builder.Append("Signature:")
                               .Append(entry->Signature & kSignatureMask)
                               .Append((entry->Signature & kDuplicatedMask) != 0 ? "-dup" : string.Empty)
                               .Append(" Hash: ")
                               .Append(entry->Hash)
                               .Append(" Node: ")
                               .Append(this.owner.Handle(node).ToDebugString())
                               .Append(" => ")
                               .Append(this.owner.ToDebugString(node))
                               .AppendLine();
                    }
                }

                builder.AppendLine("}");

                return builder.ToString();
            }

            public KeyCollection Keys
            {
                get { return new KeyCollection(this); }
            }

            public ValueCollection Values
            {
                get { return new ValueCollection(this); }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void ResizeIfNeeded()
            {
                if (this._state.Size >= NextGrowthThreshold)
                {
                    Grow(Capacity * 2);
                }
            }

            private void Grow(int newCapacity)
            {
                Contract.Requires(newCapacity >= Capacity);
                Contract.Ensures((Capacity & (Capacity - 1)) == 0);

                // Calculate the amount of pages the old table uses. 
                int entriesPerPage = GetEntriesPerPage(_tx);
                var pages = GetPagesToAllocate(entriesPerPage, this.Capacity);

                // Allocate the new table storage.
                var newPage = Allocate(_tx, _root, newCapacity);
                var oldPage = _tx.GetPage(_root.Table);
                Rehash(oldPage, newPage, newCapacity); // Rehash into the new pages. 

                // Free the old table storage.
                for (int i = 0; i < pages; i++)
                    _tx.FreePage(_root.Table + i);

                // Change the current table with the new one and use the new table state. 
                _root.Table = newPage.PageNumber;
                _state = new PrefixTreeTablePageMutableState(_tx, newPage);
            }

            private void Shrink(int newCapacity)
            {
                Contract.Requires(newCapacity > this._state.Size);
                Contract.Ensures(this.NumberOfUsed < this.Capacity);

                // Calculate the amount of pages the old table uses. 
                int entriesPerPage = GetEntriesPerPage(_tx);
                var pages = GetPagesToAllocate(entriesPerPage, this.Capacity);

                // Calculate the next power of 2.
                newCapacity = Math.Max(Bits.NextPowerOf2(newCapacity), InitialCapacity);

                var newPage = Allocate(_tx, _root, newCapacity);
                var oldPage = _tx.GetPage(_root.Table);
                Rehash(oldPage, newPage, newCapacity);

                // Free the old table storage.
                for (int i = 0; i < pages; i++)
                    _tx.FreePage(oldPage.PageNumber + i);

                // Change the current table with the new one and use the new table state. 
                _root.Table = newPage.PageNumber;
                _state = new PrefixTreeTablePageMutableState(_tx, newPage);
            }

            private void Rehash(Page oldPage, Page newTable, int newCapacity)
            {
                var oldHeader = (PrefixTreeTablePageHeader*)oldPage.Pointer;

                // Rehashing, no allocation happens here. 
                uint destCapacity = (uint)newCapacity;

                var size = 0;

                long sourceTablePage = oldPage.PageNumber;
                long destinationTablePage = newTable.PageNumber;

                int sourceCapacity = oldHeader->Capacity;

                for (int it = 0; it < sourceCapacity; it++)
                {
                    var sourceEntry = this.ReadEntry(it, sourceTablePage);
                    if (sourceEntry->Hash < kDeleted)
                    {
                        // Clear the duplicated signal. 
                        uint signature = sourceEntry->Signature & kSignatureMask;

                        uint numProbes = 1;
                        uint bucket = sourceEntry->Hash % destCapacity;

                        var newEntry = this.ReadEntry((int)bucket, destinationTablePage);
                        while (!(newEntry->Hash == kUnused))
                        {
                            if (newEntry->Signature == signature)
                            {
                                newEntry->Signature |= kDuplicatedMask;
                                WriteEntry((int)bucket, newTable.PageNumber, newEntry->Hash, newEntry->Signature, newEntry->NodePtr);
                            }

                            bucket = (bucket + numProbes) % destCapacity;
                            newEntry = this.ReadEntry((int)bucket, destinationTablePage);

                            numProbes++;
                        }

                        WriteEntry((int)bucket, destinationTablePage, sourceEntry->Hash, signature, sourceEntry->NodePtr);

                        size++;
                    }                        
                }

                Debug.Assert(oldHeader->Size == size);

                var newHeader = (PrefixTreeTablePageHeader*)newTable.Pointer;
                newHeader->Capacity = newCapacity;
                newHeader->Size = size;                

                newHeader->NumberOfUsed = size;
                newHeader->NumberOfDeleted = 0;
                newHeader->NextGrowthThreshold = newCapacity * 4 / LoadFactor;

                ValidateRehashContent(oldHeader, newHeader);
            }

            // [Conditional("VALIDATE")]
            private void ValidateRehashContent(PrefixTreeTablePageHeader* srcTable, PrefixTreeTablePageHeader* destTable)
            {
                int srcNonTombstones = 0;
                for (int i = 0; i < srcTable->Capacity; i++)
                {
                    var srcEntry = ReadEntry(i, srcTable->PageNumber);
                    if (srcEntry->Hash < kDeleted)
                        srcNonTombstones++;
                }

                int destNonTombstones = 0;
                for (int i = 0; i < destTable->Capacity; i++)
                {
                    var destEntry = ReadEntry(i, destTable->PageNumber);
                    if (destEntry->Hash < kDeleted)
                        destNonTombstones++;
                }

                if (srcNonTombstones != destNonTombstones)
                    throw new VoronUnrecoverableErrorException("After rehash the table has different values");

            }

            public sealed class KeyCollection : IEnumerable<BitVector>, IEnumerable
            {
                private InternalTable dictionary;

                public KeyCollection(InternalTable dictionary)
                {
                    Contract.Requires(dictionary != null);

                    this.dictionary = dictionary;
                }

                public Enumerator GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }

                public void CopyTo(BitVector[] array, int index)
                {
                    if (array == null)
                        throw new ArgumentNullException(nameof(array), "The array cannot be null");

                    if (index < 0 || index > array.Length)
                        throw new ArgumentOutOfRangeException(nameof(index));

                    if (array.Length - index < dictionary.Count)
                        throw new ArgumentException("The array plus the offset is too small.");

                    int count = dictionary.Capacity;

                    for (int i = 0; i < count; i++)
                    {
                        var entry = dictionary.ReadEntry(i);
                        if (entry->Hash < kDeleted)
                        {
                            var node = (Node*)dictionary.owner.ReadNodeByName(entry->NodePtr);
                            array[index++] = dictionary.owner.Handle(node);
                        }                            
                    }
                }

                public int Count
                {
                    get { return dictionary.Count; }
                }


                IEnumerator<BitVector> IEnumerable<BitVector>.GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }

                IEnumerator IEnumerable.GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }


                public struct Enumerator : IEnumerator<BitVector>, IEnumerator
                {
                    private InternalTable dictionary;
                    private int index;
                    private BitVector currentKey;

                    internal Enumerator(InternalTable dictionary)
                    {
                        this.dictionary = dictionary;
                        index = 0;
                        currentKey = default(BitVector);
                    }

                    public void Dispose()
                    {
                    }

                    public bool MoveNext()
                    {
                        var count = dictionary.Capacity;

                        while (index < count)
                        {
                            var entry = dictionary.ReadEntry(index);
                            if (entry->Hash < kDeleted)
                            {
                                var node = dictionary.owner.ReadNodeByName(entry->NodePtr);
                                currentKey = dictionary.owner.Handle(node);
                                index++;

                                return true;
                            }
                            index++;
                        }

                        index = count + 1;
                        currentKey = default(BitVector);
                        return false;
                    }

                    public BitVector Current
                    {
                        get
                        {
                            return currentKey;
                        }
                    }

                    Object System.Collections.IEnumerator.Current
                    {
                        get
                        {
                            if (index == 0 || (index == dictionary.Count + 1))
                                throw new InvalidOperationException("Cant happen.");

                            return currentKey;
                        }
                    }

                    void System.Collections.IEnumerator.Reset()
                    {
                        index = 0;
                        currentKey = default(BitVector);
                    }
                }
            }



            public sealed class ValueCollection : IEnumerable<long>, IEnumerable
            {
                private InternalTable dictionary;

                public ValueCollection(InternalTable dictionary)
                {
                    Contract.Requires(dictionary != null);

                    this.dictionary = dictionary;
                }

                public Enumerator GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }

                public void CopyTo(long[] array, int index)
                {
                    if (array == null)
                        throw new ArgumentNullException(nameof(array), "The array cannot be null");

                    if (index < 0 || index > array.Length)
                        throw new ArgumentOutOfRangeException(nameof(index));

                    if (array.Length - index < dictionary.Count)
                        throw new ArgumentException("The array plus the offset is too small.");

                    int count = dictionary.Capacity;
                   
                    for (int i = 0; i < count; i++)
                    {
                        var entry = dictionary.ReadEntry(index);
                        if (entry->Hash < kDeleted)
                            array[index++] = entry->NodePtr;
                    }
                }

                public int Count
                {
                    get { return dictionary.Count; }
                }

                IEnumerator<long> IEnumerable<long>.GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }

                IEnumerator IEnumerable.GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }


                public struct Enumerator : IEnumerator<long>, IEnumerator
                {
                    private InternalTable dictionary;
                    private int index;
                    private long currentValue;

                    internal Enumerator(InternalTable dictionary)
                    {
                        this.dictionary = dictionary;
                        index = 0;
                        currentValue = kInvalidNode;
                    }

                    public void Dispose()
                    {
                    }

                    public bool MoveNext()
                    {
                        var count = dictionary.Capacity;
                                               
                        while (index < count)
                        {
                            var entry = dictionary.ReadEntry(index);
                            if (entry->Hash < kDeleted)
                            {
                                currentValue = entry->NodePtr;
                                index++;
                                return true;
                            }
                            index++;
                        }

                        index = count + 1;
                        currentValue = kInvalidNode;
                        return false;
                    }

                    public long Current
                    {
                        get
                        {
                            return currentValue;
                        }
                    }
                    Object IEnumerator.Current
                    {
                        get
                        {
                            if (index == 0 || (index == dictionary.Count + 1))
                                throw new InvalidOperationException("Cant happen.");

                            return currentValue;
                        }
                    }

                    void IEnumerator.Reset()
                    {
                        index = 0;
                        currentValue = kInvalidNode;
                    }
                }
            }

            internal static uint CalculateHashForBits(BitVector vector, Hashing.Iterative.XXHash32Block state, int length = int.MaxValue, int lcp = int.MaxValue)
            {
                length = Math.Min(vector.Count, length); // Ensure we use the proper value.

                int words = length / BitVector.BitsPerWord;
                int remaining = length % BitVector.BitsPerWord;

                ulong remainingWord = 0;
                int shift = 0;
                if (remaining != 0)
                {
                    remainingWord = vector.GetWord(words); // Zero addressing ensures we get the next byte.
                    shift = BitVector.BitsPerWord - remaining;
                }

                unsafe
                {
                    fixed (ulong* bitsPtr = vector.Bits)
                    {
                        uint hash = Hashing.Iterative.XXHash32.CalculateInline((byte*)bitsPtr, words * sizeof(ulong), state, lcp / BitVector.BitsPerByte);

                        remainingWord = ((remainingWord) >> shift) << shift;
                        ulong intermediate = Hashing.CombineInline(remainingWord, ((ulong)remaining) << 32 | (ulong)hash);

                        hash = (uint)intermediate ^ (uint)(intermediate >> 32);

#if DETAILED_DEBUG_H
                        Console.WriteLine(string.Format("\tHash -> Hash: {0}, Remaining: {2}, Bits({1}), Vector:{3}", hash, remaining, remainingWord, vector.SubVector(0, length).ToBinaryString()));
#endif
                        return hash;
                    }
                }
            }

            private void Verify(Func<bool> action)
            {

                if (action() == false)
                    throw new Exception("Fail");
            }

            internal void VerifyStructure()
            {
                int count = 0;
                Entry* entry;

                for (int i = 0; i < this.Capacity; i++)
                {
                    entry = ReadEntry(i);
                    if (entry->NodePtr != kInvalidNode)
                    {
                        var node = this.owner.ReadNodeByName(entry->NodePtr);

                        var handle = this.owner.Handle(node);
                        var hashState = Hashing.Iterative.XXHash32.Preprocess(handle.Bits);

                        uint hash = CalculateHashForBits(handle, hashState);

                        int position = GetExactPosition(handle, handle.Count, hash & kSignatureMask);

                        var entryAtPosition = ReadEntry(position);

                        Verify(() => position != -1);
                        Verify(() => entry->NodePtr == entryAtPosition->NodePtr);
                        Verify(() => entry->Hash == (hash & kHashMask & kSignatureMask));
                        Verify(() => i == position);

                        count++;
                    }
                }

                Verify(() => count == this.Count);

                if (count == 0)
                    return;

                var overallHashes = new HashSet<uint>();
                int start = 0;
                int first = -1;

                entry = ReadEntry(start);
                while (entry->NodePtr != kInvalidNode)
                {
                    Verify(() => entry->Hash != kUnused || entry->Hash != kDeleted);
                    Verify(() => entry->Signature != kUnused);

                    start = (start + 1) % Capacity;
                    entry = ReadEntry(start);
                }

                do
                {
                    entry = ReadEntry(start);
                    while (entry->NodePtr == kInvalidNode)
                    {
                        Verify(() => entry->Hash == kUnused || entry->Hash == kDeleted);
                        Verify(() => entry->Signature == kUnused);

                        start = (start + 1) % Capacity;
                        entry = ReadEntry(start);
                    }

                    if (first == -1)
                        first = start;
                    else if (first == start)
                        break;

                    int end = start;

                    entry = ReadEntry(end);
                    while (entry->NodePtr != kInvalidNode)
                    {
                        Verify(() => entry->Hash != kUnused || entry->Hash != kDeleted);
                        Verify(() => entry->Signature != kUnused && entry->Signature != kDuplicatedMask);

                        end = (end + 1) % Capacity;
                        entry = ReadEntry(end);
                    }

                    var hashesSeen = new HashSet<uint>();
                    var signaturesSeen = new HashSet<uint>();

                    for (int pos = end; pos != start;)
                    {
                        pos = (pos - 1) % Capacity;
                        if (pos < 0)
                            break;

                        entry = ReadEntry(pos);
                        bool newSignature = signaturesSeen.Add(entry->Signature & kSignatureMask);
                        Verify(() => newSignature != ((entry->Signature & kDuplicatedMask) != 0));
                        hashesSeen.Add(entry->Hash);
                    }

                    foreach (var hash in hashesSeen)
                    {
                        bool added = overallHashes.Add(hash);
                        Verify(() => added);
                    }

                    start = end;
                }
                while (true);
            }

            private static class BlockCopyMemoryHelper
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static void Memset(Entry* pointer, int entries, Entry value)
                {
                    int block = 64, index = 0;
                    int length = Math.Min(block, entries);

                    //Fill the initial array
                    while (index < length)
                        pointer[index++] = value;

                    length = entries;
                    while (index < length)
                    {
                        int bytesToCopy = Math.Min(block, (length - index)) * sizeof(Entry);

                        Memory.Copy((byte*)(pointer + index), (byte*)pointer, bytesToCopy);

                        index += block;
                        block *= 2;
                    }
                }
            }
        }
    }
}
