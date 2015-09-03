using Sparrow.Binary;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Collections
{
    partial class ZFastTrieSortedSet<TKey, TValue> where TKey : IEquatable<TKey>
    {
        internal class ZFastNodesTable
        {
            private readonly ZFastTrieSortedSet<TKey, TValue> owner;       

            public const int InvalidNodePosition = -1;

            private const uint kDeleted = 0xFFFFFFFE;
            private const uint kUnused = 0xFFFFFFFF;            

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

            // TLoadFactor4 - controls hash map load. 4 means 100% load, ie. hashmap will grow
            // when number of items == capacity. Default value of 6 means it grows when
            // number of items == capacity * 3/2 (6/4). Higher load == tighter maps, but bigger
            // risk of collisions.
            private static int tLoadFactor = 6;

            private struct Entry
            {
                public uint Hash;
                public uint Signature;
                public Internal Node;

                public Entry(uint hash, uint signature, Internal node)
                {
                    this.Hash = hash;
                    this.Signature = signature;
                    this.Node = node;
                }
            }

            private Entry[] _entries;

            private int _capacity;

            private int _initialCapacity; // This is the initial capacity of the dictionary, we will never shrink beyond this point.
            private int _size; // This is the real counter of how many items are in the hash-table (regardless of buckets)
            private int _numberOfUsed; // How many used buckets. 
            private int _numberOfDeleted; // how many occupied buckets are marked deleted
            private int _nextGrowthThreshold;

            public int Capacity
            {
                get { return _capacity; }
            }

            public int Count
            {
                get { return _size; }
            }

            public ZFastNodesTable(ZFastTrieSortedSet<TKey, TValue> owner)
                : this(kInitialCapacity, owner)
            {}

            public ZFastNodesTable(int initialBucketCount, ZFastTrieSortedSet<TKey, TValue> owner)
            {
                this.owner = owner;

                // Calculate the next power of 2.
                int newCapacity = initialBucketCount >= kMinCapacity ? initialBucketCount : kMinCapacity;
                newCapacity = Bits.NextPowerOf2(newCapacity);

                this._initialCapacity = newCapacity;

                // Initialization
                this._entries = new Entry[newCapacity];
                BlockCopyMemoryHelper.Memset(this._entries, new Entry(kUnused, kUnused, default(Internal)));

                this._capacity = newCapacity;

                this._numberOfUsed = 0;
                this._numberOfDeleted = 0;
                this._size = 0;

                this._nextGrowthThreshold = _capacity * 4 / tLoadFactor;
            }


            public void Add(Internal node, uint signature)
            {
                ResizeIfNeeded();                

                // We shrink the signature to the proper size (31 bits)
                signature = signature & kSignatureMask;                

                int hash = GetInternalHashCode(signature);
                int bucket = hash % _capacity;

                uint uhash = (uint)hash;
                int numProbes = 1;
                do
                {
                    if (_entries[bucket].Signature == signature)
                        _entries[bucket].Signature |= kDuplicatedMask;

                    uint nHash = _entries[bucket].Hash;
                    if (nHash == kUnused)
                    {
                        _numberOfUsed++;
                        _size++;

                        goto SET;
                    }
                    else if (nHash == kDeleted)
                    {
                        _numberOfDeleted--;
                        _size++;

                        goto SET;
                    }

                    bucket = (bucket + numProbes) % _capacity;
                    numProbes++;
                }
                while (true);

            SET:
                this._entries[bucket].Hash = uhash;
                this._entries[bucket].Signature = signature;
                this._entries[bucket].Node = node;

#if DETAILED_DEBUG_H
                Console.WriteLine(string.Format("Add: {0}, Bucket: {1}, Signature: {2}", node.ToDebugString(this.owner), bucket, signature));
#endif
#if VERIFY
                VerifyStructure();
#endif
            }

            public void Replace(Internal oldNode, Internal newNode, uint signature)
            {                
                // We shrink the signature to the proper size (30 bits)
                signature = signature & kSignatureMask;

                int hash = GetInternalHashCode(signature);
                int pos = hash % _capacity;

                int numProbes = 1;

                while (this._entries[pos].Node != oldNode)
                {
                    pos = (pos + numProbes) % _capacity;
                    numProbes++;
                }

                Debug.Assert(this._entries[pos].Node != null);
                Debug.Assert(this._entries[pos].Hash == (uint) hash );
                Debug.Assert(this._entries[pos].Node.Handle(this.owner).CompareTo(newNode.Handle(this.owner)) == 0);

                this._entries[pos].Node = newNode;

#if DETAILED_DEBUG_H
                Console.WriteLine(string.Format("Old: {0}, Bucket: {1}, Signature: {2}", oldNode.ToDebugString(this.owner), pos, hash, signature));
                Console.WriteLine(string.Format("New: {0}", newNode.ToDebugString(this.owner)));
#endif

#if VERIFY
                VerifyStructure();
#endif

            }

            public int GetPosition(BitVector key, int prefixLength, uint signature, bool isExact)
            {
                // We shrink the signature to the proper size (30 bits)
                signature = signature & kSignatureMask;

                int position;
                if (isExact)
                {
                    position = GetExactPosition(key, prefixLength, signature);
                }
                else
                {
                    position = GetPosition(key, prefixLength, signature);
                }

                return position;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private int GetExactPosition(BitVector key, int prefixLength, uint signature)
            {
                int pos = GetInternalHashCode(signature) % _capacity;
                
                int numProbes = 1;

                uint nSignature;
                do
                {
                    nSignature = this._entries[pos].Signature;                    

                    if ((nSignature & kSignatureMask) == signature)
                    {
                        var node = this._entries[pos].Node;
                        if (node.GetHandleLength(this.owner) == prefixLength && key.IsPrefix(node.ReferencePtr.Name(this.owner), prefixLength))
                            return pos;
                    }

                    pos = (pos + numProbes) % _capacity;
                    numProbes++;

                    Debug.Assert(numProbes < 100);
                }
                while (nSignature != kUnused);

                return -1;
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private int GetPosition(BitVector key, int prefixLength, uint signature)
            {
                int pos = GetInternalHashCode(signature) % _capacity;

                int numProbes = 1;

                uint nSignature;
                do
                {
                    nSignature = this._entries[pos].Signature;

                    if ((nSignature & kSignatureMask) == signature)
                    {
                        var node = this._entries[pos].Node;
                        if ((nSignature & kDuplicatedMask) == 0 || (node.GetHandleLength(this.owner) == prefixLength && key.IsPrefix(node.ReferencePtr.Name(this.owner), prefixLength)))
                        {                           
                            return pos;
                        }
                            
                    }
                       
                    pos = (pos + numProbes) % _capacity;
                    numProbes++;

                    Debug.Assert(numProbes < 100);
                }
                while (nSignature != kUnused);

                return -1;
            }

            public Internal this[int position]
            {
                get
                {
                    return this._entries[position].Node;
                }
            }

            internal string DumpNodesTable(ZFastTrieSortedSet<TKey, TValue> tree)
            {
                var builder = new StringBuilder();

                bool first = true;
                builder.Append("After Insertion. NodesTable: {");
                foreach (var node in this.Values)
                {
                    if (!first)
                        builder.Append(", ");

                    builder.Append(node.Handle(tree).ToDebugString())
                           .Append(" => ")
                           .Append(node.ToDebugString(tree));

                    first = false;
                }
                builder.Append("} Root: ")
                       .Append(tree.Root.ToDebugString(tree));

                return builder.ToString();
            }

            internal string DumpTable()
            {
                var builder = new StringBuilder();

                bool first = true;
                builder.AppendLine("NodesTable: {");

                for ( int i = 0; i < this._entries.Length; i++ )
                {  
                    var entry = this._entries[i];
                    if (entry.Hash != kUnused)
                    {
                        var node = entry.Node;

                        builder.Append("Signature:")
                               .Append(entry.Signature & kSignatureMask)
                               .Append((entry.Signature & kDuplicatedMask) != 0 ? "-dup" : string.Empty)
                               .Append(" Hash: ")
                               .Append(entry.Hash)
                               .Append(" Node: ")
                               .Append(node.Handle(this.owner).ToDebugString())
                               .Append(" => ")
                               .Append(node.ToDebugString(this.owner))
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

            public void Clear()
            {
                throw new NotImplementedException();
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private void ResizeIfNeeded()
            {
                if (_size >= _nextGrowthThreshold)
                {
                    Grow(_capacity * 2);
                }
            }

            private void Grow(int newCapacity)
            {
                Contract.Requires(newCapacity >= _capacity);
                Contract.Ensures((_capacity & (_capacity - 1)) == 0);

                var entries = new Entry[newCapacity];
                BlockCopyMemoryHelper.Memset(entries, new Entry(kUnused, kUnused, default(Internal)));

                Rehash(entries);
            }


            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private int GetInternalHashCode(uint hash)
            {
                return (int)(hash.GetHashCode() & kHashMask);
            }

            private void Rehash(Entry[] entries)
            {
                uint capacity = (uint)entries.Length;

                var size = 0;

                for (int it = 0; it < _entries.Length; it++)
                {
                    uint hash = _entries[it].Hash;
                    if (hash >= kDeleted) // No interest for the process of rehashing, we are skipping it.
                        continue;

                    uint signature = _entries[it].Signature & kSignatureMask;

                    uint bucket = hash % capacity;

                    uint numProbes = 1;
                    while (!(entries[bucket].Hash == kUnused))
                    {                        
                        if (entries[bucket].Signature == signature)
                            entries[bucket].Signature |= kDuplicatedMask;
                       
                        bucket = (bucket + numProbes) % capacity;
                        numProbes++;
                    }

                    entries[bucket].Hash = hash;
                    entries[bucket].Signature = signature;
                    entries[bucket].Node = _entries[it].Node;

                    size++;
                }

                this._capacity = entries.Length;
                this._size = size;
                this._entries = entries;

                this._numberOfUsed = size;
                this._numberOfDeleted = 0;

                this._nextGrowthThreshold = _capacity * 4 / tLoadFactor;
            }


            public sealed class KeyCollection : IEnumerable<BitVector>, IEnumerable
            {
                private ZFastNodesTable dictionary;

                public KeyCollection(ZFastNodesTable dictionary)
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
                        throw new ArgumentNullException("The array cannot be null", "array");

                    if (index < 0 || index > array.Length)
                        throw new ArgumentOutOfRangeException("index");

                    if (array.Length - index < dictionary.Count)
                        throw new ArgumentException("The array plus the offset is too small.");

                    int count = dictionary._capacity;
                    var entries = dictionary._entries;

                    for (int i = 0; i < count; i++)
                    {
                        if (entries[i].Hash < kDeleted)
                            array[index++] = entries[i].Node.Handle(dictionary.owner);
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


                [Serializable]
                public struct Enumerator : IEnumerator<BitVector>, IEnumerator
                {
                    private ZFastNodesTable dictionary;
                    private int index;
                    private BitVector currentKey;

                    internal Enumerator(ZFastNodesTable dictionary)
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
                        var count = dictionary._capacity;

                        var entries = dictionary._entries;
                        while (index < count)
                        {
                            if (entries[index].Hash < kDeleted)
                            {
                                currentKey = entries[index].Node.Handle(dictionary.owner);
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



            public sealed class ValueCollection : IEnumerable<Internal>, IEnumerable
            {
                private ZFastNodesTable dictionary;

                public ValueCollection(ZFastNodesTable dictionary)
                {
                    Contract.Requires(dictionary != null);

                    this.dictionary = dictionary;
                }

                public Enumerator GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }

                public void CopyTo(Internal[] array, int index)
                {
                    if (array == null)
                        throw new ArgumentNullException("The array cannot be null", "array");

                    if (index < 0 || index > array.Length)
                        throw new ArgumentOutOfRangeException("index");

                    if (array.Length - index < dictionary.Count)
                        throw new ArgumentException("The array plus the offset is too small.");

                    int count = dictionary._capacity;

                    var entries = dictionary._entries;
                    for (int i = 0; i < count; i++)
                    {
                        if (entries[i].Hash < kDeleted)
                            array[index++] = entries[i].Node;
                    }
                }

                public int Count
                {
                    get { return dictionary.Count; }
                }

                IEnumerator<Internal> IEnumerable<Internal>.GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }

                IEnumerator IEnumerable.GetEnumerator()
                {
                    return new Enumerator(dictionary);
                }


                [Serializable]
                public struct Enumerator : IEnumerator<Internal>, IEnumerator
                {
                    private ZFastNodesTable dictionary;
                    private int index;
                    private Internal currentValue;

                    internal Enumerator(ZFastNodesTable dictionary)
                    {
                        this.dictionary = dictionary;
                        index = 0;
                        currentValue = default(Internal);
                    }

                    public void Dispose()
                    {
                    }

                    public bool MoveNext()
                    {
                        var count = dictionary._capacity;

                        var entries = dictionary._entries;
                        while (index < count)
                        {
                            if (entries[index].Hash < kDeleted)
                            {
                                currentValue = entries[index].Node;
                                index++;
                                return true;
                            }
                            index++;
                        }

                        index = count + 1;
                        currentValue = default(Internal);
                        return false;
                    }

                    public Internal Current
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
                        currentValue = default(Internal);
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
                        uint hash = Hashing.Iterative.XXHash32.Calculate((byte*)bitsPtr, words * sizeof(ulong), state, lcp / BitVector.BitsPerByte);

                        uint* combine = stackalloc uint[4];
                        ((ulong*)combine)[0] = ((remainingWord) >> shift) << shift;
                        combine[2] = (uint)remaining;
                        combine[3] = hash;

                        hash = Hashing.XXHash32.Calculate((byte*)combine, 4 * sizeof(uint));
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

            internal void VerifyStructure ()
            {
                int count = 0;
                for ( int i = 0; i < this._entries.Length; i++ )
                {
                    if ( this._entries[i].Node != null )
                    {
                        var handle = this._entries[i].Node.Handle ( this.owner);                        
                        var hashState = Hashing.Iterative.XXHash32.Preprocess(handle.Bits);
                        
                        uint hash = CalculateHashForBits( handle, hashState );

                        int position = GetExactPosition(handle, handle.Count, hash & kSignatureMask);

                        Verify(() => position != -1);
                        Verify(() => this._entries[i].Node == this._entries[position].Node);
                        Verify(() => this._entries[i].Hash == GetInternalHashCode(hash & kSignatureMask));
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
                while (this._entries[start].Node != null)
                {
                    Verify(() => this._entries[start].Hash != kUnused);
                    Verify(() => this._entries[start].Signature != kUnused);

                    start = (start + 1) % _capacity;
                }

                do
                {
                    while (this._entries[start].Node == null)
                    {
                        Verify(() => this._entries[start].Hash == kUnused);
                        Verify(() => this._entries[start].Signature == kUnused);

                        start = (start + 1) % _capacity;
                    }

                    if (first == -1)
                        first = start;
                    else if (first == start)
                        break;

                    int end = start;
                    while (this._entries[end].Node != null)
                    {
                        Verify(() => this._entries[end].Hash != kUnused);
                        Verify(() => this._entries[end].Signature != kUnused && this._entries[end].Signature != kDuplicatedMask);

                        end = (end + 1) % _capacity;
                    }

                    var hashesSeen = new HashSet<uint>();
                    var signaturesSeen = new HashSet<uint>();

                    for ( int pos = end; pos != start; )
                    {
                        pos = (pos - 1) % _capacity;
                        if (pos < 0)
                            break;

                        bool newSignature = signaturesSeen.Add(this._entries[pos].Signature & kSignatureMask);
                        Verify(() => newSignature != ((this._entries[pos].Signature & kDuplicatedMask) != 0));
                        hashesSeen.Add(this._entries[pos].Hash);
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

            private class BlockCopyMemoryHelper
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                public static void Memset(Entry[] array, Entry value)
                {
                    int block = 64, index = 0;
                    int length = Math.Min(block, array.Length);

                    //Fill the initial array
                    while (index < length)
                    {
                        array[index++] = value;
                    }

                    length = array.Length;
                    while (index < length)
                    {
                        Array.Copy(array, 0, array, index, Math.Min(block, (length - index)));
                        index += block;

                        block *= 2;
                    }
                }
            }
        }


    }


}
