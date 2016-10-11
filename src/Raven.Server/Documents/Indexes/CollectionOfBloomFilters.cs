using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Data.BTrees;

namespace Raven.Server.Documents.Indexes
{
    public class CollectionOfBloomFilters
    {
        private readonly int _singleBloomFilterCapacity;
        private readonly TransactionOperationContext _context;
        private BloomFilter[] _filters;
        private BloomFilter _currentFilter;
        private readonly Tree _tree;

        private CollectionOfBloomFilters(int singleBloomFilterCapacity, Tree tree, TransactionOperationContext context)
        {
            _singleBloomFilterCapacity = singleBloomFilterCapacity;
            _context = context;
            _tree = tree;
        }

        public int Count
        {
            get
            {
                if (_filters == null)
                    return 0;

                return _filters.Length;
            }
        }

        public BloomFilter this[int index] => _filters[index];

        public static unsafe CollectionOfBloomFilters Load(int singleBloomFilterCapacity, TransactionOperationContext indexContext)
        {
            var tree = indexContext.Transaction.InnerTransaction.CreateTree("IndexedDocs");
            var collection = new CollectionOfBloomFilters(singleBloomFilterCapacity, tree, indexContext);
            using (var it = tree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        var reader = it.CreateReaderForCurrent();

                        Debug.Assert(reader.Length == singleBloomFilterCapacity);

                        collection.AddFilter(new BloomFilter(it.CurrentKey.Clone(indexContext.Allocator, ByteStringType.Immutable), reader.Base, tree, writeable: false));
                    } while (it.MoveNext());
                }
            }

            collection.Initialize();

            return collection;
        }

        private void Initialize()
        {
            if (_filters != null && _filters.Length > 0)
            {
                ExpandFiltersIfNecessary();
                return;
            }

            AddFilter(CreateNewFilter(0));
        }

        private unsafe BloomFilter CreateNewFilter(int number)
        {
            Slice key;
            Slice.From(_context.Allocator, number.ToString("D9"), out key);
            var ptr = _tree.DirectAdd(key, BloomFilter.PtrSize);
            return new BloomFilter(key, ptr, _tree, writeable: true);
        }

        private void AddFilter(BloomFilter filter)
        {
            if (_filters == null)
            {
                _filters = new BloomFilter[1];
                _filters[0] = _currentFilter = filter;
                return;
            }

            var length = _filters.Length;
            Array.Resize(ref _filters, length + 1);
            _filters[length - 1].MakeReadOnly();
            _filters[length] = _currentFilter = filter;
        }

        public bool Add(LazyStringValue key)
        {
            if (_filters.Length == 1)
            {
                if (_currentFilter.Add(key))
                {
                    ExpandFiltersIfNecessary();
                    return true;
                }

                return false;
            }

            for (var i = 0; i < _filters.Length - 1; i++)
            {
                if (_filters[i].Contains(key))
                    return false;
            }

            if (_currentFilter.Add(key))
            {
                ExpandFiltersIfNecessary();
                return true;
            }

            return false;
        }

        private void ExpandFiltersIfNecessary()
        {
            if (_currentFilter.Count < _singleBloomFilterCapacity)
                return;

            AddFilter(CreateNewFilter(_filters.Length));
        }

        public unsafe class BloomFilter
        {
            public const int CountSize = sizeof(int);
            public const int PtrSize = 16 * 1024 * 1024;
            public const int Capacity = 10000000;

            private const long M = (PtrSize - CountSize) * BitVector.BitsPerByte;
            private const long K = 10;

            private readonly Slice _key;
            private readonly byte* _basePtr;
            private readonly Tree _tree;
            private byte* _dataPtr;
            private int* _countPtr;

            public int Count { get; private set; }

            public bool ReadOnly { get; private set; }

            public bool Writeable { get; private set; }

            public BloomFilter(Slice key, byte* basePtr, Tree tree, bool writeable)
            {
                _key = key;
                _basePtr = basePtr;
                _tree = tree;
                Writeable = writeable;

                Initialize(basePtr);
            }

            public bool Add(LazyStringValue key)
            {
                if (ReadOnly)
                    throw new InvalidOperationException($"Cannot add new item to a read-only bloom filter '{_key}'.");

                var newItem = false;
                var primaryHash = CalculatePrimaryHash(key);
                var secondaryHash = CalculateSecondaryHash(key);
                for (ulong i = 0; i < K; i++)
                {
                    // Dillinger and Manolios double hashing
                    var finalHash = (primaryHash + (i * secondaryHash)) % M;

                    var ptrPosition = finalHash / BitVector.BitsPerByte;
                    var bitPosition = (int)(finalHash % BitVector.BitsPerByte);

                    var bitValue = GetBit(_dataPtr, ptrPosition, bitPosition);
                    if (bitValue)
                        continue;

                    if (Writeable == false)
                        MakeWriteable();

                    SetBitToTrue(_dataPtr, ptrPosition, bitPosition);
                    newItem = true;
                }

                if (newItem)
                {
                    *_countPtr = ++Count;
                    return true;
                }

                return false;
            }

            public bool Contains(LazyStringValue key)
            {
                var primaryHash = CalculatePrimaryHash(key);
                var secondaryHash = CalculateSecondaryHash(key);
                for (ulong i = 0; i < K; i++)
                {
                    // Dillinger and Manolios double hashing
                    var finalHash = (primaryHash + (i * secondaryHash)) % M;

                    var ptrPosition = finalHash / BitVector.BitsPerByte;
                    var bitPosition = (int)(finalHash % BitVector.BitsPerByte);

                    var bitValue = GetBit(_dataPtr, ptrPosition, bitPosition);
                    if (bitValue == false)
                        return false;
                }

                return true;
            }

            public void MakeReadOnly()
            {
                ReadOnly = true;
            }

            private void MakeWriteable()
            {
                if (Writeable)
                    return;

                var ptr = _tree.DirectAdd(_key, PtrSize);
                UnmanagedMemory.Copy(ptr, _basePtr, PtrSize);

                Initialize(ptr);

                Writeable = true;
            }

            private void Initialize(byte* ptr)
            {
                _countPtr = (int*)ptr;
                _dataPtr = ptr + CountSize;
                Count = *_countPtr;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static ulong CalculatePrimaryHash(LazyStringValue key)
            {
                return Hashing.XXHash64.CalculateInline(key.Buffer, (ulong)key.Size, seed: 1);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static ulong CalculateSecondaryHash(LazyStringValue key)
            {
                return Hashing.XXHash64.CalculateInline(key.Buffer, (ulong)key.Size, seed: 1337);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool GetBit(byte* ptr, ulong ptrPosition, int bitPosition)
            {
                return (ptr[ptrPosition] & (1 << bitPosition)) != 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static void SetBitToTrue(byte* ptr, ulong ptrPosition, int bitPosition)
            {
                ptr[ptrPosition] |= (byte)(1 << bitPosition);
            }
        }
    }
}