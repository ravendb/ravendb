using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Binary;
using Sparrow.Global;
using Sparrow.Platform;
using Sparrow.Threading;
using Sparrow.Utils;

using static Sparrow.DisposableExceptions;
using static Sparrow.PortableExceptions;

#if MEM_GUARD
using Sparrow.Platform;
#endif


namespace Sparrow.Json
{
    public sealed unsafe class ArenaMemoryAllocator : IDisposableQueryable, IDisposable
    {
        internal const int MaxArenaSize = 1024 * 1024 * 1024;
        private static readonly int? SingleAllocationSizeLimit = PlatformDetails.Is32Bits ? 8 * Constants.Size.Megabyte : (int?)null;

        private byte* _ptrStart;
        private byte* _ptrCurrent;

        private long _allocated;
        private long _used;

        private List<Tuple<IntPtr, long, NativeMemory.ThreadStats>> _olderBuffers;

        private struct FreeSection
        {
#pragma warning disable 649
            public FreeSection* Previous;
            public int SizeInBytes;
#pragma warning restore 649
        }

        private readonly FreeSection*[] _freed = new FreeSection*[32];

        private readonly SingleUseFlag _isDisposed = new();
        private NativeMemory.ThreadStats _allocatingThread;
        private readonly int _initialSize;

        public long TotalUsed;

        public bool AvoidOverAllocation;

        public long Allocated
        {
            get
            {
                var totalAllocation = _allocated;
                if (_olderBuffers != null)
                {
                    foreach (var olderBuffer in _olderBuffers)
                    {
                        totalAllocation += olderBuffer.Item2;
                    }
                }
                return totalAllocation;
            }
        }

        public ArenaMemoryAllocator(SharedMultipleUseFlag lowMemoryFlag, int initialSize = 1024 * 1024)
        {
            _initialSize = initialSize;
            _ptrStart = _ptrCurrent = NativeMemory.AllocateMemory(initialSize, out _allocatingThread);
            _allocated = initialSize;
            _used = 0;
            TotalUsed = 0;
        }

        public bool GrowAllocation(AllocatedMemoryData allocation, int sizeIncrease)
        {
            var newAllocationSize = allocation.SizeInBytes + sizeIncrease;
            if (newAllocationSize > MaxArenaSize)
                return false;

            // we need to keep the total allocation size as power of 2
            var totalAllocation = Bits.PowerOf2(newAllocationSize);
            var index = Bits.MostSignificantBit(totalAllocation) - 1;
            if (_freed[index] != null)
            {
                // once we increase the size of the allocation,
                // it might return to the array of fragmented chunks (_free) for future reuse
                // however it isn't going to be used since we always request the initial allocation
                // and only after that we request to grow its size, so it will remain forever in the pool
                // https://ayende.com/blog/197825-C/production-postmortem-efficiency-all-the-way-to-out-of-memory-error?key=e100f37887a0471db8f78c1a5f831f88
                return false;
            }

            byte* end = allocation.Address + allocation.SizeInBytes;
            var distance = end - _ptrCurrent;
            if (distance != 0)
                return false;

            sizeIncrease = totalAllocation - allocation.SizeInBytes;

            if (_used + sizeIncrease > _allocated)
                return false;

            _ptrCurrent += sizeIncrease;
            _used += sizeIncrease;
            TotalUsed += sizeIncrease;
            allocation.SizeInBytes += sizeIncrease;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public AllocatedMemoryData Allocate(int size)
        {
            ThrowIfDisposed(this);

            ThrowIfNull<InvalidOperationException>(_ptrStart, "Attempt to allocate from reset arena without calling renew");
            ThrowIf<ArgumentOutOfRangeException>(size < 0, "Size cannot be negative");
            
            if (size > MaxArenaSize)
                Throw<ArgumentOutOfRangeException>($"Requested size {size} while maximum size is {MaxArenaSize}");
            
#if MEM_GUARD
            return new AllocatedMemoryData
            {
                Address = ElectricFencedMemory.Allocate(size),
                SizeInBytes = size
            };
#else
            
            size = Bits.PowerOf2(Math.Max(sizeof(FreeSection), size));

            var index = Bits.MostSignificantBit(size) - 1;
            if (_freed[index] != null)
            {
                var section = _freed[index];
                _freed[index] = section->Previous;

                return new AllocatedMemoryData((byte*)section, section->SizeInBytes);
            }

            if (_used + size > _allocated)
            {
                GrowArena(size);
            }

            AllocatedMemoryData allocation = new(_ptrCurrent, size);

            _ptrCurrent += size;
            _used += size;
            TotalUsed += size;

            return allocation;
#endif
        }

        private void GrowArena(int requestedSize)
        {
            ThrowIf<ArgumentOutOfRangeException>(requestedSize > MaxArenaSize, $"Requested arena resize to {requestedSize} while current size is {_allocated} and maximum size is {MaxArenaSize}");
            
            long newSize = GetPreferredSize(requestedSize);
            if (newSize > MaxArenaSize)
                newSize = MaxArenaSize;

            byte* newBuffer;
            NativeMemory.ThreadStats thread;
            try
            {
                newBuffer = NativeMemory.AllocateMemory(newSize, out thread);
            }
            catch (OutOfMemoryException oom)
                when (oom.Data?.Contains("Recoverable") != true) // this can be raised if the commit charge is low
            {
                // we were too eager with memory allocations?
                newBuffer = NativeMemory.AllocateMemory(requestedSize, out thread);
                newSize = requestedSize;
            }

            // Save the old buffer pointer to be released when the arena is reset
            _olderBuffers ??= new List<Tuple<IntPtr, long, NativeMemory.ThreadStats>>();
            _olderBuffers.Add(Tuple.Create(new IntPtr(_ptrStart), _allocated, _allocatingThread));

            _allocatingThread = thread;

            _allocated = newSize;

            _ptrStart = newBuffer;
            _ptrCurrent = _ptrStart;
            _used = 0;
        }

        private long GetPreferredSize(int requestedSize)
        {
            if (AvoidOverAllocation || PlatformDetails.Is32Bits)
                return ApplyLimit(Bits.PowerOf2(requestedSize));

            // we need the next allocation to cover at least the next expansion (also doubling)
            // so we'll allocate 3 times as much as was requested, or as much as we already have
            // the idea is that a single allocation can server for multiple (increasing in size) calls
            return ApplyLimit(Math.Max(Bits.PowerOf2(requestedSize) * 3L, _initialSize));

            long ApplyLimit(long size)
            {
                if (SingleAllocationSizeLimit == null)
                    return size;

                if (size > SingleAllocationSizeLimit.Value)
                {
                    var sizeInMb = requestedSize / Constants.Size.Megabyte + (requestedSize % Constants.Size.Megabyte == 0 ? 0 : 1);
                    return sizeInMb * Constants.Size.Megabyte;
                }

                return size;
            }
        }

        public void RenewArena()
        {
            if (_ptrStart != null)
                return;
            _ptrStart = _ptrCurrent = NativeMemory.AllocateMemory(_allocated, out _allocatingThread);
            _used = 0;
            TotalUsed = 0;
        }

        public void ResetArena()
        {
            // Reset current arena buffer
            _ptrCurrent = _ptrStart;
            Array.Clear(_freed, 0, _freed.Length);

            if (_olderBuffers == null)
            {
                // there were no new allocations in this round
                if (_allocated / 2 > _used && _allocated > _initialSize * 2)
                {
                    // we used less than half the memory we have, so let us reduce it

                    if (_ptrStart != null)
                        NativeMemory.Free(_ptrStart, _allocated, _allocatingThread);

                    _allocated = Math.Max(_allocated / 2, _initialSize);
                    _ptrCurrent = _ptrStart = null;
                }
                _used = 0;
                TotalUsed = 0;
                return;
            }
            // Free old buffers not being used anymore
            foreach (var unusedBuffer in _olderBuffers)
            {
                _used += unusedBuffer.Item2;
                NativeMemory.Free((byte*)unusedBuffer.Item1, unusedBuffer.Item2, unusedBuffer.Item3);
            }
            _olderBuffers = null;
            if (_used <= _allocated)
            {
                _used = 0;
                TotalUsed = 0;
                return;
            }
            // we'll likely need more memory in the next round, let us increase the size we hold on to

            if (_ptrStart != null)
                NativeMemory.Free(_ptrStart, _allocated, _allocatingThread);

            // we'll allocate some multiple of the currently allocated amount, that will prevent big spikes in memory
            // consumption and has the worst case usage of doubling memory utilization

            var newSize = (_used / _allocated + (_used % _allocated == 0 ? 0 : 1)) * _allocated;

            _allocated = newSize;
            _used = 0;
            TotalUsed = 0;
            if (_allocated > MaxArenaSize)
                _allocated = MaxArenaSize;
            if (SingleAllocationSizeLimit != null && _allocated > SingleAllocationSizeLimit.Value)
                _allocated = SingleAllocationSizeLimit.Value;
            _ptrCurrent = _ptrStart = null;
        }

        ~ArenaMemoryAllocator()
        {
            try
            {
                Dispose(false);
            }
            catch (ObjectDisposedException)
            {
                // This is expected, we might be calling the finalizer on an object that
                // was already disposed, we don't want to error here because of this
            }
        }

        public override string ToString()
        {
            return $"Allocated {Sizes.Humane(Allocated)}, Used {Sizes.Humane(_used)}";
        }

        public void Dispose()
        {
            Dispose(true);
        }

        public void Dispose(bool disposing)
        {
            if (!_isDisposed?.Raise() ?? true)
                return;

            if (disposing)
                Monitor.Enter(this);

            try
            {
                if (_olderBuffers != null)
                {
                    foreach (var unusedBuffer in _olderBuffers)
                    {
                        NativeMemory.Free((byte*)unusedBuffer.Item1, unusedBuffer.Item2, unusedBuffer.Item3);
                    }
                    _olderBuffers = null;
                }
                if (_ptrStart != null)
                {
                    NativeMemory.Free(_ptrStart, _allocated, _allocatingThread);
                    _ptrStart = null;
                }

                GC.SuppressFinalize(this);
            }
            finally
            {
                if (disposing)
                    Monitor.Exit(this);
            }
        }

        public bool IsDisposed => _isDisposed ?? true;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(AllocatedMemoryData allocation)
        {
            if (IsDisposed) return;

            var address = allocation.Address;

#if DEBUG
            Debug.Assert(allocation.IsReturned == false, "allocation.IsReturned == false");
            allocation.IsReturned = true;
#endif

#if MEM_GUARD
#if MEM_GUARD_STACK
            if (allocation.FreedBy == null)
                allocation.FreedBy = Environment.StackTrace;
#endif
            ElectricFencedMemory.Free(address);
#else

            if (address != _ptrCurrent - allocation.SizeInBytes ||
                address < _ptrStart)
            {
                // we have fragmentation, so'll just store the values that we need here
                // in the memory we just freed :-)

                // note that this fragmentation will be healed by the call to ResetArena
                // trying to do this on the fly is too expensive.

                Debug.Assert(Bits.PowerOf2(allocation.SizeInBytes) == allocation.SizeInBytes,
                    "Allocation size must always be a power of two"
                );
                Debug.Assert(allocation.SizeInBytes >= sizeof(FreeSection));

                var index = Bits.MostSignificantBit(allocation.SizeInBytes) - 1;
                var section = (FreeSection*)address;
                section->SizeInBytes = allocation.SizeInBytes;
                section->Previous = _freed[index];
                _freed[index] = section;
                return;
            }

#if DEBUG
            var current = _ptrCurrent;

            var ptrAddress = new IntPtr(address);
            var ptrCurrent = new IntPtr(current);
            Debug.Assert(ptrAddress != ptrCurrent, $"address != current ({ptrAddress} != {ptrCurrent} [{nameof(_ptrCurrent)} = {new IntPtr(_ptrCurrent)}], allocated: {_allocated}, used: {_used})");
#endif
            // since the returned allocation is at the end of the arena, we can just move
            // the pointer back
            _used -= allocation.SizeInBytes;
            TotalUsed -= allocation.SizeInBytes;
            _ptrCurrent -= allocation.SizeInBytes;
#endif
        }

        internal sealed class IntPtrComparer : IComparer<IntPtr>
        {
            public static IntPtrComparer Instance = new IntPtrComparer();

            public int Compare(IntPtr x, IntPtr y)
            {
                return Math.Sign((x.ToInt64() - y.ToInt64()));
            }
        }
    }

    public sealed unsafe class AllocatedMemoryData : IDisposableQueryable
    {
        public int SizeInBytes;
        public int ContextGeneration;

        public JsonOperationContext Parent;
        public NativeMemory.ThreadStats AllocatingThread;

        public AllocatedMemoryData(byte* address, int sizeInBytes)
        {
            SizeInBytes = sizeInBytes;
            Address = address;
        }
        public Span<byte> AsSpan()
        {
            return new Span<byte>(Address, SizeInBytes);
        }

#if MEM_GUARD_STACK || TRACK_ALLOCATED_MEMORY_DATA
        public string AllocatedBy = Environment.StackTrace;
        public string FreedBy;
#endif

#if !DEBUG
        public readonly byte* Address;
        bool IDisposableQueryable.IsDisposed => false;
#else
        public bool IsLongLived;
        public bool IsReturned;

        private byte* _address;

        bool IDisposableQueryable.IsDisposed => IsLongLived == false &&
                                                Parent != null &&
                                                ContextGeneration != Parent.Generation ||
                                                IsReturned;

        public byte* Address
        {
            get
            {
                ThrowIfDisposed(this);
                return _address;
            }

            private set
            {
                ThrowIfDisposed(this);
                _address = value;
            }
        }

#endif
    }

    internal sealed unsafe class UnmanagedMemoryManager : MemoryManager<byte>
    {
        private readonly byte* _address;
        private readonly int _length;

        public UnmanagedMemoryManager(byte* pointer, int length)
        {
            _address = pointer;
            _length = length;
        }

        public override Memory<byte> Memory => CreateMemory(_length);

        public override Span<byte> GetSpan() => new(_address, _length);

        public override MemoryHandle Pin(int elementIndex = 0)
        {
            if (elementIndex < 0 || elementIndex >= _length)
                throw new ArgumentOutOfRangeException(nameof(elementIndex));

            return new MemoryHandle(_address + elementIndex);
        }

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
        }
    }
}
