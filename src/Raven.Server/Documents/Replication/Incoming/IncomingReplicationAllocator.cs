using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Utils;

namespace Raven.Server.Documents.Replication.Incoming
{
    public unsafe class IncomingReplicationAllocator : IDisposable
    {
        private readonly long _maxSizeForContextUseInBytes;
        private readonly long _minSizeToAllocateNonContextUseInBytes;
        public long TotalDocumentsSizeInBytes { get; private set; }

        private List<Allocation> _nativeAllocationList;
        private Allocation _currentAllocation;
        private readonly ByteStringContext _allocator;

        public IncomingReplicationAllocator(ByteStringContext allocator, Size? maxSizeToSend)
        {
            _allocator = allocator;
            var maxSizeForContextUse = maxSizeToSend * 2 ?? new Size(128, SizeUnit.Megabytes);

            _maxSizeForContextUseInBytes = maxSizeForContextUse.GetValue(SizeUnit.Bytes);
            var minSizeToNonContextAllocationInMb = PlatformDetails.Is32Bits ? 4 : 16;
            _minSizeToAllocateNonContextUseInBytes = new Size(minSizeToNonContextAllocationInMb, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes);
        }

        public byte* AllocateMemory(int size)
        {
            TotalDocumentsSizeInBytes += size;
            if (TotalDocumentsSizeInBytes <= _maxSizeForContextUseInBytes)
            {
                _allocator.Allocate(size, out var output);
                return output.Ptr;
            }

            if (_currentAllocation == null || _currentAllocation.Free < size)
            {
                // first allocation or we don't have enough space on the currently allocated chunk

                // there can be a document that is larger than the minimum
                var sizeToAllocate = Math.Max(size, _minSizeToAllocateNonContextUseInBytes);

                var allocation = new Allocation(sizeToAllocate);
                if (_nativeAllocationList == null)
                    _nativeAllocationList = new List<Allocation>();

                _nativeAllocationList.Add(allocation);
                _currentAllocation = allocation;
            }

            return _currentAllocation.GetMemory(size);
        }

        public void Dispose()
        {
            if (_nativeAllocationList == null)
                return;

            foreach (var allocation in _nativeAllocationList)
            {
                allocation.Dispose();
            }
        }

        private class Allocation : IDisposable
        {
            private readonly byte* _ptr;
            private readonly long _allocationSize;
            private readonly NativeMemory.ThreadStats _threadStats;
            private long _used;
            public long Free => _allocationSize - _used;

            public Allocation(long allocationSize)
            {
                _ptr = NativeMemory.AllocateMemory(allocationSize, out var threadStats);
                _allocationSize = allocationSize;
                _threadStats = threadStats;
            }

            public byte* GetMemory(long size)
            {
                ThrowOnPointerOutOfRange(size);

                var mem = _ptr + _used;
                _used += size;
                return mem;
            }

            [Conditional("DEBUG")]
            private void ThrowOnPointerOutOfRange(long size)
            {
                if (_used + size > _allocationSize)
                    throw new InvalidOperationException(
                        $"Not enough space to allocate the requested size: {new Size(size, SizeUnit.Bytes)}, " +
                        $"used: {new Size(_used, SizeUnit.Bytes)}, " +
                        $"total allocation size: {new Size(_allocationSize, SizeUnit.Bytes)}");
            }

            public void Dispose()
            {
                NativeMemory.Free(_ptr, _allocationSize, _threadStats);
            }
        }
    }
}
