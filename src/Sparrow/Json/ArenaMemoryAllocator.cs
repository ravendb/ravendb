using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Utils;

namespace Sparrow.Json
{
    public unsafe class ArenaMemoryAllocator : IDisposable
    {
        private const int MaxArenaSize = 1024 * 1024 * 1024;

        private byte* _ptrStart;
        private byte* _ptrCurrent;

        private long _allocated;
        private long _used;

        private List<Tuple<IntPtr, long, NativeMemory.ThreadStats>> _olderBuffers;
#if !MEM_GUARD
        private SortedList<IntPtr, int> _fragements;
#endif
        private bool _isDisposed;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ArenaMemoryAllocator>("ArenaMemoryAllocator");
        private NativeMemory.ThreadStats _allocatingThread;
        private readonly int _initialSize;

        public long TotalUsed;

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

        public ArenaMemoryAllocator(int initialSize = 1024 * 1024)
        {
            _initialSize = initialSize;
            _ptrStart = _ptrCurrent = NativeMemory.AllocateMemory(initialSize, out _allocatingThread);
            _allocated = initialSize;
            _used = 0;
            TotalUsed = 0;

            if (Logger.IsInfoEnabled)
                Logger.Info($"ArenaMemoryAllocator was created with initial capacity of {initialSize:#,#;;0} bytes");
        }


        public bool GrowAllocation(AllocatedMemoryData allocation, int sizeIncrease)
        {
            var end = allocation.Address + allocation.SizeInBytes;
            var distance = end - _ptrCurrent;
            if (distance != 0)
                return false;

            if (_used + sizeIncrease > _allocated)
                return false;

            _ptrCurrent += sizeIncrease;
            _used += sizeIncrease;
            TotalUsed += sizeIncrease;
            allocation.SizeInBytes += sizeIncrease;
            return true;
        }

        public AllocatedMemoryData Allocate(int size)
        {
            if (_isDisposed)
                ThrowAlreadyDisposedException();
            if(_ptrStart == null)
                ThrowInvalidAllocateFromResetWithoutRenew();

#if MEM_GUARD
            var allocation = new AllocatedMemoryData
            {
                Address = ElectricFencedMemory.Allocate(size),
                SizeInBytes = size
            };
#else
            if (_used + size > _allocated)
                GrowArena(size);

#if DEBUG
            if (_fragements != null)
            {
                Debug.Assert(_fragements.ContainsKey((IntPtr)_ptrCurrent) == false);
            }
#endif

            var allocation = new AllocatedMemoryData()
            {
                SizeInBytes = size,
                Address = _ptrCurrent
            };

            _ptrCurrent += size;
            _used += size;
            TotalUsed += size;
#endif
            
            return allocation;
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
            if (requestedSize >= MaxArenaSize)
                throw new ArgumentOutOfRangeException(nameof(requestedSize));

            // we need the next allocation to cover at least the next expansion (also doubling)
            // so we'll allocate 3 times as much as was requested, or as much as we already have
            // the idea is that a single allocation can server for multiple (increasing in size) calls
            long newSize = Math.Max(Bits.NextPowerOf2(requestedSize) * 3, _initialSize);
            if (newSize > MaxArenaSize)
                newSize = MaxArenaSize;

            if (Logger.IsInfoEnabled)
            {
                if (newSize > 512 * 1024 * 1024)
                    Logger.Info($"Arena main buffer reached size of {newSize:#,#;0} bytes (previously {_allocated:#,#;0} bytes), check if you forgot to reset the context. From now on we grow this arena in 1GB chunks.");
                Logger.Info($"Allocated additional {newSize:#,#;0} because we need {requestedSize:#,#;0}.");
            }

            NativeMemory.ThreadStats thread;
            var newBuffer = NativeMemory.AllocateMemory(newSize, out thread);

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
#if !MEM_GUARD
            _fragements?.Clear();
#endif

            if (_olderBuffers == null)
            {
                // there were no new allocations in this round
                if (_allocated / 2 > _used && _allocated > _initialSize * 2)
                {
                    // we used less than half the memory we have, so let us reduce it

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

            NativeMemory.Free(_ptrStart, _allocated, _allocatingThread);

            // we'll allocate some multiple of the currently allocated amount, that will prevent big spikes in memory 
            // consumption and has the worst case usage of doubling memory utilization

            var newSize = (_used / _allocated + (_used % _allocated == 0 ? 0 : 1)) * _allocated;

            _allocated = newSize;
            _used = 0;
            TotalUsed = 0;
            if (_allocated > MaxArenaSize)
                _allocated = MaxArenaSize;
            _ptrCurrent = _ptrStart = null;
        }

        ~ArenaMemoryAllocator()
        {
            if (Logger.IsInfoEnabled)
                Logger.Info("ArenaMemoryAllocator wasn't properly disposed");

            Dispose();
        }

        public override string ToString()
        {
            return $"Allocated {Sizes.Humane(Allocated)}, Used {Sizes.Humane(_used)}";
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;
            _isDisposed = true;

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

        public void Return(AllocatedMemoryData allocation)
        {
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
                if (_fragements == null)
                {
                    _fragements = new SortedList<IntPtr, int>(IntPtrComarer.Instance);
                }

                // we have fragmentation, let us try to heal it 
                _fragements.Add((IntPtr)address, allocation.SizeInBytes);
                return;
            }
            // since the returned allocation is at the end of the arena, we can just move
            // the pointer back
            _used -= allocation.SizeInBytes;
            TotalUsed -= allocation.SizeInBytes;
            _ptrCurrent -= allocation.SizeInBytes;

            if (_fragements == null)
                return;

            // let us try to heal fragmentation at this point
            while (_fragements.Count > 0)
            {
                var highestAddress = (byte*)(_fragements.Keys[_fragements.Count - 1]);
                if (highestAddress != _ptrCurrent - allocation.SizeInBytes)
                    break;

                var sizeInBytes = _fragements.Values[_fragements.Count - 1];

                _fragements.RemoveAt(_fragements.Count - 1);
                if (highestAddress < _ptrStart)
                {
                    // this is from another segment, probably, currently we'll just ignore it,
                    // we might want to track if all the memory from a previous segment has been
                    // released, and then free it, but not for now
                    continue;
                }
                _used -= sizeInBytes;
                TotalUsed -= sizeInBytes;
                _ptrCurrent -= sizeInBytes;
            }
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

#if MEM_GUARD_STACK
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