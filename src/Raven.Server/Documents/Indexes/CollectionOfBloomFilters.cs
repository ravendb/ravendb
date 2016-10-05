using System;
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

        public static unsafe CollectionOfBloomFilters Load(int singleBloomFilterCapacity, TransactionOperationContext indexContext)
        {
            var tree = indexContext.Transaction.InnerTransaction.CreateTree("IndexedDocs");
            var collection = new CollectionOfBloomFilters(singleBloomFilterCapacity, tree, indexContext);
            var numberOfFilters = tree.State.NumberOfEntries;
            using (var it = tree.Iterate(prefetch: false))
            {
                var count = 0;
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        collection.AddFilter(count == numberOfFilters - 1
                            ? CreateCurrentFilter(count, it.CreateReaderForCurrent().Base, tree, indexContext)
                            : new BloomFilter(count, it.CreateReaderForCurrent().Base));

                        count++;
                    } while (it.MoveNext());
                }
            }

            collection.Initialize();

            return collection;
        }

        private static unsafe BloomFilter CreateCurrentFilter(int number, byte* src, Tree tree, TransactionOperationContext context)
        {
            Slice key;
            using (Slice.From(context.Allocator, number.ToString("D9"), out key))
            {
                var ptr = tree.DirectAdd(key, BloomFilter.PtrSize);
                Memory.Copy(ptr, src, BloomFilter.PtrSize);
                return new BloomFilter(number, ptr);
            }
        }

        private void Initialize()
        {
            if (_filters != null && _filters.Length > 0)
            {
                ExpandFiltersIfNecessary();
                return;
            }

            _filters = new BloomFilter[1];
            _filters[0] = CreateNewFilter(0);
        }

        private unsafe BloomFilter CreateNewFilter(int number)
        {
            Slice key;
            using (Slice.From(_context.Allocator, number.ToString("D9"), out key))
            {
                var ptr = _tree.DirectAdd(key, BloomFilter.PtrSize);
                Memory.Set(ptr, 0, BloomFilter.PtrSize);
                return new BloomFilter(number, ptr);
            }
        }

        private void AddFilter(BloomFilter filter)
        {
            if (_filters == null)
            {
                _filters = new BloomFilter[1];
                _filters[0] = filter;
                return;
            }

            var length = _filters.Length;
            Array.Resize(ref _filters, length + 1);
            _filters[length - 1].ReadOnly();
            _filters[length] = filter;
        }

        public bool Add(LazyStringValue key)
        {
            bool result;
            BloomFilter filter;
            if (_filters.Length == 1)
            {
                filter = _filters[0];
                result = filter.Add(key);
                if (result)
                    ExpandFiltersIfNecessary();

                return result;
            }

            for (var i = 0; i < _filters.Length - 1; i++)
            {
                if (_filters[i].Contains(key))
                    return false;
            }

            filter = _filters[_filters.Length - 1];
            result = filter.Add(key);
            if (result)
                ExpandFiltersIfNecessary();

            return result;
        }

        private void ExpandFiltersIfNecessary()
        {
            if (_filters[_filters.Length - 1].Count < _singleBloomFilterCapacity)
                return;

            AddFilter(CreateNewFilter(_filters.Length));
        }

        public unsafe class BloomFilter
        {
            public const int PtrSize = 16 * 1024 * 1024;
            public const int Capacity = 10000000;

            private const long M = PtrSize * BitVector.BitsPerByte;
            private const long K = 10;

            private readonly int _key;
            private readonly byte* _ptr;
            private bool _readOnly;

            public int Count { get; private set; }

            public BloomFilter(int key, byte* ptr)
            {
                _key = key;
                _ptr = ptr;
            }

            public bool Add(LazyStringValue key)
            {
                if (_readOnly)
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

                    var bitValue = GetBit(_ptr, ptrPosition, bitPosition);
                    if (bitValue)
                        continue;

                    SetBitToTrue(_ptr, ptrPosition, bitPosition);
                    newItem = true;
                }

                if (newItem)
                {
                    Count++;
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

                    var bitValue = GetBit(_ptr, ptrPosition, bitPosition);
                    if (bitValue == false)
                        return false;
                }

                return true;
            }

            public void ReadOnly()
            {
                _readOnly = true;
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