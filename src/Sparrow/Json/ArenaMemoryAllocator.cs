using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Binary;
using Sparrow.Global;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Threading;
#if MEM_GUARD
using Sparrow.Platform;
#endif
using Sparrow.Utils;

namespace Sparrow.Json
{
    public unsafe class ArenaMemoryAllocator : IDisposable
    {
        public const int MaxArenaSize = 1024 * 1024 * 1024;

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

        private readonly SingleUseFlag _isDisposed = new SingleUseFlag();
        private NativeMemory.ThreadStats _allocatingThread;
        private readonly int _initialSize;

        public long TotalUsed;

        public bool AvoidOverAllocation;

        private readonly SharedMultipleUseFlag _lowMemoryFlag;

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
            _lowMemoryFlag = lowMemoryFlag;
        }

        public bool GrowAllocation(AllocatedMemoryData allocation, int sizeIncrease)
        {
            byte* end = allocation.Address + allocation.SizeInBytes;
            var distance = end - _ptrCurrent;
            if (distance != 0)
                return false;

            // we need to keep the total allocation size as power of 2
            sizeIncrease = Bits.NextPowerOf2(allocation.SizeInBytes + sizeIncrease) - allocation.SizeInBytes;

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
            if (_isDisposed)
                goto ErrorDisposed;

            if (_ptrStart == null)
                goto ErrorResetted;

#if MEM_GUARD
            return new AllocatedMemoryData
            {
                Address = ElectricFencedMemory.Allocate(size),
                SizeInBytes = size
            };
#else
            size = Bits.NextPowerOf2(Math.Max(sizeof(FreeSection), size));

            AllocatedMemoryData allocation;

            var index = Bits.MostSignificantBit(size) - 1;
            if (_freed[index] != null)
            {
                var section = _freed[index];
                _freed[index] = section->Previous;

                allocation = new AllocatedMemoryData
                {
                    Address = (byte*)section,
                    SizeInBytes = section->SizeInBytes
                };
                goto Return;
            }

            if (_used + size > _allocated)
            {
                GrowArena(size);
            }

            allocation = new AllocatedMemoryData
            {
                SizeInBytes = size,
                Address = _ptrCurrent
            };

            _ptrCurrent += size;
            _used += size;
            TotalUsed += size;

            Return: return allocation;
#endif

            ErrorDisposed: ThrowAlreadyDisposedException();
            ErrorResetted: ThrowInvalidAllocateFromResetWithoutRenew();
            return null; // Will never happen.
        }

        private static void ThrowInvalidAllocateFromResetWithoutRenew()
        {
            throw new InvalidOperationException("Attempt to allocate from reset arena without calling renew");
        }

        private void ThrowAlreadyDisposedException()
        {
            throw new ObjectDisposedException("This ArenaMemoryAllocator is already disposed");
        }

        private void GrowArena(int requestedSize)
        {
            if (requestedSize <0 || requestedSize >= MaxArenaSize)
                throw new ArgumentOutOfRangeException(nameof(requestedSize), requestedSize, "Allocation size is not valid: " + requestedSize);

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
            if (_olderBuffers == null)
                _olderBuffers = new List<Tuple<IntPtr, long, NativeMemory.ThreadStats>>();
            _olderBuffers.Add(Tuple.Create(new IntPtr(_ptrStart), _allocated, _allocatingThread));

            _allocatingThread = thread;

            _allocated = newSize;

            _ptrStart = newBuffer;
            _ptrCurrent = _ptrStart;
            _used = 0;
        }

        private int GetPreferredSize(int requestedSize)
        {
            if (AvoidOverAllocation)
                return ApplyLimit(Bits.NextPowerOf2(requestedSize));

            // we need the next allocation to cover at least the next expansion (also doubling)
            // so we'll allocate 3 times as much as was requested, or as much as we already have
            // the idea is that a single allocation can server for multiple (increasing in size) calls
            return ApplyLimit(Math.Max(Bits.NextPowerOf2(requestedSize) * 3, _initialSize));

            int ApplyLimit(int size)
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
                Dispose();
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
            if (!_isDisposed.Raise())
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


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(AllocatedMemoryData allocation)
        {
            if (_isDisposed)
                return;

            var address = allocation.Address;
#if DEBUG
            Debug.Assert(address != _ptrCurrent);
            Debug.Assert(allocation.IsReturned==false);
            allocation.IsReturned = true;
#endif

#if MEM_GUARD
#if MEM_GUARD_STACK
            if(allocation.FreedBy == null)
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

                Debug.Assert(Bits.NextPowerOf2(allocation.SizeInBytes) == allocation.SizeInBytes,
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
            // since the returned allocation is at the end of the arena, we can just move
            // the pointer back
            _used -= allocation.SizeInBytes;
            TotalUsed -= allocation.SizeInBytes;
            _ptrCurrent -= allocation.SizeInBytes;
#endif
        }

        public class IntPtrComarer : IComparer<IntPtr>
        {
            public static IntPtrComarer Instance = new IntPtrComarer();

            public int Compare(IntPtr x, IntPtr y)
            {
                return Math.Sign((x.ToInt64() - y.ToInt64()));
            }
        }
    }

    public unsafe class AllocatedMemoryData
    {
        public int SizeInBytes;
        public int ContextGeneration;

        public JsonOperationContext Parent;
        public NativeMemory.ThreadStats AllocatingThread;

#if MEM_GUARD_STACK || TRACK_ALLOCATED_MEMORY_DATA
        public string AllocatedBy = Environment.StackTrace;
        public string FreedBy;
#endif

#if !DEBUG
        public byte* Address;
#else
        public bool IsLongLived;
        public bool IsReturned;
        private byte* _address;
        public byte* Address
        {
            get
            {
                if (IsLongLived == false &&
                    Parent != null &&
                    ContextGeneration != Parent.Generation ||
                    IsReturned)
                    ThrowObjectDisposedException();

                return _address;
            }
            set
            {
                if (IsLongLived == false &&
                    Parent != null &&
                    ContextGeneration != Parent.Generation ||
                    IsReturned)
                    ThrowObjectDisposedException();

                _address = value;
            }
        }

        private void ThrowObjectDisposedException()
        {
           throw new ObjectDisposedException(nameof(AllocatedMemoryData));
        }
#endif

    }
}
