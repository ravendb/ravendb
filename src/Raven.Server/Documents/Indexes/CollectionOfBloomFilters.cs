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
    public class CollectionOfBloomFilters : IDisposable
    {
        public enum Mode
        {
            X64,
            X86
        }

        private readonly TransactionOperationContext _context;
        private BloomFilter[] _filters;
        private BloomFilter _currentFilter;
        private Mode _mode;
        private readonly Tree _tree;

        private CollectionOfBloomFilters(Mode mode, Tree tree, TransactionOperationContext context)
        {
            _mode = mode;
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

        public static CollectionOfBloomFilters Load(Mode mode, TransactionOperationContext indexContext)
        {
            var tree = indexContext.Transaction.InnerTransaction.CreateTree("IndexedDocs");
            var collection = new CollectionOfBloomFilters(mode, tree, indexContext);
            using (var it = tree.Iterate(prefetch: false))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        var reader = it.CreateReaderForCurrent();

                        BloomFilter filter;
                        switch (reader.Length)
                        {
                            case BloomFilter32.PtrSize:
                                filter = new BloomFilter32(it.CurrentKey.Clone(indexContext.Allocator, ByteStringType.Immutable), reader, tree);
                                break;
                            case BloomFilter64.PtrSize:
                                filter = new BloomFilter64(it.CurrentKey.Clone(indexContext.Allocator, ByteStringType.Immutable), reader, tree);
                                break;
                            default:
                                throw new InvalidOperationException($"Unsupported bloom filter size ({reader.Length}) for '{it.CurrentKey}' filter.");
                        }

                        collection.AddFilter(filter);
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

            AddFilter(CreateNewFilter(0, _mode));
        }

        internal BloomFilter CreateNewFilter(int number, Mode mode)
        {
            Slice key;
            Slice.From(_context.Allocator, number.ToString("D9"), out key);

            if (mode == Mode.X64)
            {
                var ptr64 = _tree.DirectAdd(key, BloomFilter64.PtrSize);
                return new BloomFilter64(key, ptr64, _tree);
            }

            var ptr32 = _tree.DirectAdd(key, BloomFilter32.PtrSize);
            return new BloomFilter32(key, ptr32, _tree);
        }

        internal void AddFilter(BloomFilter filter)
        {
            if (_filters == null)
            {
                _mode = filter is BloomFilter64 ? Mode.X64 : Mode.X86; // first filter determines actual mode
                _filters = new BloomFilter[1];
                _filters[0] = _currentFilter = filter;
                return;
            }

            switch (_mode)
            {
                case Mode.X64:
                    if (filter is BloomFilter32)
                        throw new InvalidOperationException("Cannot add 32-bit filter in 64-bit mode.");
                    break;
                case Mode.X86:
                    if (filter is BloomFilter64)
                        throw new InvalidOperationException("Cannot add 64-bit filter in 32-bit mode.");
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
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
            if (_currentFilter.Count < _currentFilter.Capacity)
                return;

            AddFilter(CreateNewFilter(_filters.Length, _mode));
        }

        public void Dispose()
        {
            if (_filters == null)
                return;

            foreach (var filter in _filters)
            {
                filter.Dispose();
            }
        }

        public class BloomFilter32 : BloomFilter
        {
            public const int PtrSize = 32 * 1024;
            public const int MaxCapacity = 25000;

            private const ulong M = (PtrSize - CountSize) * BitVector.BitsPerByte;

            public BloomFilter32(Slice key, Tree.DirectAddScope addScope, Tree tree)
                : base(key, addScope, tree, M, PtrSize, MaxCapacity)
            {
            }

            public BloomFilter32(Slice key, ValueReader reader, Tree tree)
                : base(key, reader, tree, M, PtrSize, MaxCapacity)
            {
            }
        }

        public class BloomFilter64 : BloomFilter
        {
            public const int PtrSize = 16 * 1024 * 1024;
            public const int MaxCapacity = 10000000;

            private const ulong M = (PtrSize - CountSize) * BitVector.BitsPerByte;

            public BloomFilter64(Slice key, Tree.DirectAddScope addScope, Tree tree)
                : base(key, addScope, tree, M, PtrSize, MaxCapacity)
            {
            }

            public BloomFilter64(Slice key, ValueReader reader, Tree tree)
                : base(key, reader, tree, M, PtrSize, MaxCapacity)
            {
            }
        }

        public abstract unsafe class BloomFilter : IDisposable
        {
            public const int CountSize = sizeof(int);

            private const long K = 10;

            private readonly Slice _key;
            private readonly byte* _basePtr;
            private readonly Tree _tree;
            private readonly ulong _m;
            private readonly int _ptrSize;
            private Tree.DirectAddScope _addScope;
            private byte* _dataPtr;
            private int* _countPtr;

            public int Count { get; private set; }

            public bool ReadOnly { get; private set; }

            public bool Writeable { get; private set; }

            public readonly int Capacity;

            protected BloomFilter(Slice key, Tree.DirectAddScope addScope, Tree tree, ulong m, int ptrSize, int capacity)
                : this(key, addScope.Ptr, tree, true, m, ptrSize, capacity)
            {
                _addScope = addScope;
            }

            protected BloomFilter(Slice key, ValueReader reader, Tree tree, ulong m, int ptrSize, int capacity)
                : this(key, reader.Base, tree, false, m, ptrSize, capacity)
            {
            }

            private BloomFilter(Slice key, byte* basePtr, Tree tree, bool writeable, ulong m, int ptrSize, int capacity)
            {
                _key = key;
                _basePtr = basePtr;
                _tree = tree;
                _m = m;
                _ptrSize = ptrSize;
                Capacity = capacity;
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
                    var finalHash = (primaryHash + (i * secondaryHash)) % _m;

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
                    var finalHash = (primaryHash + (i * secondaryHash)) % _m;

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

                Debug.Assert(_addScope == null);

                _addScope = _tree.DirectAdd(_key, _ptrSize);
                UnmanagedMemory.Copy(_addScope.Ptr, _basePtr, _ptrSize);
                Initialize(_addScope.Ptr);

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

            public void Dispose()
            {
                _addScope?.Dispose();
            }
        }
    }
}