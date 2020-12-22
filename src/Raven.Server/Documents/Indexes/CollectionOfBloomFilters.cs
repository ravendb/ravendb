using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Data.BTrees;

namespace Raven.Server.Documents.Indexes
{
    public class CollectionOfBloomFilters : IDisposable
    {
        private static readonly Slice Count64Slice;
        private static readonly Slice Count32Slice;
        internal static readonly Slice VersionSlice;

        static CollectionOfBloomFilters()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Count64", ByteStringType.Immutable, out Count64Slice);
                Slice.From(ctx, "Count32", ByteStringType.Immutable, out Count32Slice);
                Slice.From(ctx, "Version", ByteStringType.Immutable, out VersionSlice);
            }
        }

        public enum Mode
        {
            X64,
            X86
        }

        private readonly TransactionOperationContext _context;
        private BloomFilter[] _filters;
        private BloomFilter _currentFilter;
        private Mode _mode;
        public readonly long Version;
        private readonly Tree _tree;
        public bool Consumed;
        private static readonly long _consumed = -1L;

        private CollectionOfBloomFilters(Mode mode, long version, Tree tree, TransactionOperationContext context)
        {
            _mode = mode;
            _context = context;
            _tree = tree;

            Version = version;
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
            var isNew = indexContext.Transaction.InnerTransaction.ReadTree("BloomFilters") == null;
            var tree = indexContext.Transaction.InnerTransaction.CreateTree("BloomFilters");
            var version = GetVersion(tree, isNew);
            var count = GetCount(tree, ref mode);
            if (count == _consumed)
            {
                Debug.Assert(mode == Mode.X86, "BloomFilters in x64 mode got consumed, should not happen and likely a bug!");

                var consumedCollection = new CollectionOfBloomFilters(mode, version, tree: null, context: null)
                {
                    Consumed = true
                };

                return consumedCollection;
            }

            var collection = new CollectionOfBloomFilters(mode, version, tree, indexContext);

            for (var i = 0; i < count; i++)
            {
                BloomFilter filter = null;
                switch (mode)
                {
                    case Mode.X64:
                        filter = new BloomFilter64(i, version, tree, writable: false, allocator: indexContext.Allocator);
                        break;

                    case Mode.X86:
                        filter = new BloomFilter32(i, version, tree, writable: false, allocator: indexContext.Allocator);
                        break;
                }

                collection.AddFilter(filter);
            }

            collection.Initialize();

            return collection;
        }

        internal static long GetVersion(Tree tree, bool isNew)
        {
            var read = tree.Read(VersionSlice);
            if (read != null)
                return read.Reader.ReadLittleEndianInt64();

            return isNew
                ? BloomFilterVersion.CurrentVersion
                : BloomFilterVersion.BaseVersion;
        }

        private static long GetCount(Tree tree, ref Mode mode)
        {
            var read = tree.Read(mode == Mode.X64 ? Count64Slice : Count32Slice);
            if (read != null)
                return read.Reader.ReadLittleEndianInt64();

            read = tree.Read(mode == Mode.X64 ? Count32Slice : Count64Slice);
            if (read != null)
            {
                mode = mode == Mode.X64 ? Mode.X86 : Mode.X64;
                return read.Reader.ReadLittleEndianInt64();
            }

            return 0;
        }

        private void Initialize()
        {
            _tree.Add(VersionSlice, Version);

            if (_filters != null && _filters.Length > 0)
            {
                ExpandFiltersIfNecessary();
                return;
            }

            AddFilter(CreateNewFilter(0, _mode));
        }

        internal BloomFilter CreateNewFilter(int number, Mode mode)
        {
            if (mode == Mode.X64)
            {
                _tree.Increment(Count64Slice, 1);

                Debug.Assert(_tree.ShouldGoToOverflowPage(BloomFilter64.PtrSize));

                return new BloomFilter64(number, Version, _tree, writable: true, allocator: _context.Allocator);
            }

            _tree.Increment(Count32Slice, 1);

            Debug.Assert(_tree.ShouldGoToOverflowPage(BloomFilter32.PtrSize));

            return new BloomFilter32(number, Version, _tree, writable: true, allocator: _context.Allocator);
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
            if (Consumed)
                return false;

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

            if (_mode == Mode.X86 && _filters.Length >= 20)
            {
                Consumed = true;
                return;
            }

            AddFilter(CreateNewFilter(_filters.Length, _mode));
        }

        public void Flush()
        {
            if (_filters == null || _filters.Length == 0)
                return;

            if (Consumed)
            {
                foreach (var filter in _filters)
                    filter.Delete();

                // mark as consumed in the storage
                _tree.Add(Count32Slice, _consumed);

                _filters = null;
                _currentFilter = null;
                return;
            }

            foreach (var filter in _filters)
                filter.Flush();
        }

        public void Dispose()
        {
        }

        public class BloomFilter32 : BloomFilter
        {
            public const int PtrSize = 32 * 1024;
            public const int MaxCapacity = 25000;

            private const ulong M = PtrSize * PtrBitVector.BitsPerByte;

            public BloomFilter32(int key, long version, Tree tree, bool writable, ByteStringContext allocator)
                : base(key, version, tree, writable, M, PtrSize, MaxCapacity, allocator)
            {
            }
        }

        public class BloomFilter64 : BloomFilter
        {
            public const int PtrSize = 16 * 1024 * 1024;
            public const int MaxCapacity = 10000000;

            private const ulong M = PtrSize * PtrBitVector.BitsPerByte;

            public BloomFilter64(int key, long version, Tree tree, bool writable, ByteStringContext allocator)
                : base(key, version, tree, writable, M, PtrSize, MaxCapacity, allocator)
            {
            }
        }

        public abstract unsafe class BloomFilter : IDisposable
        {
            private const long K = 10;

            private readonly int _key;
            private readonly long _version;
            private readonly Slice _keySlice;
            private readonly Tree _tree;
            private readonly ulong _m;
            private readonly int _ptrSize;
            private readonly uint _partitionCount;
            private readonly Dictionary<ulong, Partition> _partitions = new Dictionary<ulong, Partition>(NumericEqualityComparer.BoxedInstanceUInt64);
            private readonly ByteStringContext _allocator;
            private readonly long _initialCount;

            public long Count { get; private set; }

            public bool ReadOnly { get; private set; }

            public bool Writable { get; private set; }

            public readonly int Capacity;

            protected BloomFilter(int key, long version, Tree tree, bool writable, ulong m, int ptrSize, int capacity, ByteStringContext allocator)
            {
                _key = key;
                _version = version;
                _tree = tree;
                _m = m;
                _ptrSize = ptrSize;
                _partitionCount = (uint)Math.Ceiling(_ptrSize / (double)Partition.PartitionSize);
                Capacity = capacity;
                _allocator = allocator;
                Writable = writable;

                Slice.From(_allocator, $"{_key:D5}", out _keySlice);
                Count = _initialCount = ReadCount();
            }

            private long ReadCount()
            {
                var read = _tree.Read(_keySlice);
                if (read == null)
                    return 0;

                return read.Reader.ReadLittleEndianInt64();
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

                    var ptrPosition = finalHash / PtrBitVector.BitsPerByte;
                    var bitPosition = (int)(finalHash % PtrBitVector.BitsPerByte);

                    var partition = GetPartition(ptrPosition, out ulong partitionPtrPosition);
                    var bitValue = GetBit(partition, partitionPtrPosition, bitPosition);
                    if (bitValue)
                        continue;

                    if (partition.Writable == false)
                        MakeWritable(partition);

                    SetBitToTrue(partition, partitionPtrPosition, bitPosition);
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
                    var finalHash = (primaryHash + (i * secondaryHash)) % _m;

                    var ptrPosition = finalHash / PtrBitVector.BitsPerByte;
                    var bitPosition = (int)(finalHash % PtrBitVector.BitsPerByte);

                    var partition = GetPartition(ptrPosition, out ulong partitionPtrPosition);
                    var bitValue = GetBit(partition, partitionPtrPosition, bitPosition);
                    if (bitValue == false)
                        return false;
                }

                return true;
            }

            public void MakeReadOnly()
            {
                ReadOnly = true;
            }

            private Partition GetPartition(ulong ptrPosition, out ulong partitionPtrPosition)
            {
                ulong partitionNumber;
                switch (_version)
                {
                    case BloomFilterVersion.BaseVersion:
                        partitionNumber = ptrPosition % _partitionCount;
                        break;

                    default:
                        partitionNumber = ptrPosition / Partition.PartitionSize;
                        break;
                }

                partitionPtrPosition = ptrPosition % Partition.PartitionSize;

                return GetPartitionByNumber(partitionNumber);
            }

            private Partition GetPartitionByNumber(ulong number)
            {
                if (_partitions.TryGetValue(number, out Partition partition))
                    return partition;

                Slice.From(_allocator, $"{_key:D5}/{number:D4}", out Slice partitionKey);

                var read = _tree.Read(partitionKey);
                if (read != null)
                {
                    return _partitions[number] = new Partition
                    {
                        Writable = false,
                        Ptr = read.Reader.Base,
                        Key = partitionKey
                    };
                }

                return _partitions[number] = new Partition
                {
                    IsEmpty = true,
                    Key = partitionKey
                };
            }

            private void MakeWritable(Partition partition)
            {
                if (partition.Writable)
                    return;

                // we can safely pass the raw pointer here and dispose DirectAdd scope immediately because
                // filter's content will be written to an overflow

                Debug.Assert(_tree.ShouldGoToOverflowPage(_ptrSize));

                using (_tree.DirectAdd(partition.Key, Partition.PartitionSize, out byte* ptr))
                {
                    if (partition.IsEmpty == false)
                        Memory.Copy(ptr, partition.Ptr, Partition.PartitionSize);

                    partition.Writable = true;
                    partition.IsEmpty = false;
                    partition.Ptr = ptr;
                }

                Writable = true;
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
            private static bool GetBit(Partition partition, ulong ptrPosition, int bitPosition)
            {
                if (partition.IsEmpty)
                    return false;

                return (partition.Ptr[ptrPosition] & (1 << bitPosition)) != 0;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static void SetBitToTrue(Partition partition, ulong ptrPosition, int bitPosition)
            {
                Debug.Assert(partition.IsEmpty == false);

                partition.Ptr[ptrPosition] |= (byte)(1 << bitPosition);
            }

            public void Flush()
            {
                if (Count == _initialCount)
                    return;

                if (_tree.Llt.Environment.Options.IsCatastrophicFailureSet)
                    return; // avoid re-throwing it

                _tree.Increment(_keySlice, Count - _initialCount);
            }

            internal void Delete()
            {
                if (_tree.Llt.Environment.Options.IsCatastrophicFailureSet)
                    return; // avoid re-throwing it

                _tree.Delete(_keySlice);
            }

            public void Dispose()
            {
            }

            private class Partition
            {
                public const int PartitionSize = 64 * 1024;

                public Slice Key;

                public byte* Ptr;

                public bool Writable;

                public bool IsEmpty;
            }
        }
    }

    public static class BloomFilterVersion
    {
        public const long BaseVersion = 40_000;

        public const long PartitionFix = 42_000;

        /// <summary>
        /// Remember to bump this
        /// </summary>
        public const long CurrentVersion = PartitionFix;
    }
}
