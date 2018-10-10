using System;
using System.Collections.Generic;
using System.Reflection;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Sparrow.Global;

namespace Sparrow
{ 
    public interface IArenaGrowthStrategy
    {
        /// <summary>
        /// Every cycle the arena will ask the strategy what would be an estimated size for the next cycle (number can be even smaller than the current).
        /// </summary>
        /// <param name="allocated">The currently allocated arena memory</param>
        /// <param name="used">The memory used from this arena on the current cycle.</param>
        int GetPreferredSize(long allocated, long used);
    }

    public interface IArenaAllocatorOptions : INativeOptions
    {
        int InitialArenaSize { get; }
        int MaxArenaSize { get; }

        IArenaGrowthStrategy GrowthStrategy { get; }   
    }

    public static class ArenaAllocator
    {
        public struct DoubleGrowthStrategy : IArenaGrowthStrategy
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int GetPreferredSize(long allocated, long used)
            {
                if (used >= allocated)
                {
                    long value = used * 2;
                    if (value > int.MaxValue)
                        return int.MaxValue;

                    return (int)value;
                }

                return (int)allocated;
            }
        }

        public struct DoublePowerOf2GrowthStrategy : IArenaGrowthStrategy
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int GetPreferredSize(long allocated, long used)
            {                
                if (used >= allocated)
                {
                    long value = allocated;
                    if (value > int.MaxValue)
                        return int.MaxValue;

                    return Bits.PowerOf2((int)value);
                }

                return (int)allocated;
            }
        }

        public struct Default : IArenaAllocatorOptions
        { 
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            // TODO: Check if this call gets devirtualized. 
            public IArenaGrowthStrategy GrowthStrategy => default(DoubleGrowthStrategy);
            public int InitialArenaSize => 1 * Constants.Size.Megabyte;
            public int MaxArenaSize => 64 * Constants.Size.Megabyte;
        }
    }

    public unsafe struct ArenaAllocator<TOptions> : IAllocator<ArenaAllocator<TOptions>, Pointer>, IAllocator, IRenewable<ArenaAllocator<TOptions>>
        where TOptions : struct, IArenaAllocatorOptions
    {
        private TOptions _options;
        private NativeAllocator<TOptions> _nativeAllocator;

        private Pointer _currentBuffer;

        // The start of the current arena chunk
        private byte* _ptrStart;
        // The current position over the current arena chunk
        private byte* _ptrCurrent;

        // How many bytes has this arena section handed out to the customers.
        private long _used;

        private int _preferredSize;

        // Buffers that has been used and cannot be cleaned until a reset happens.
        private List<Pointer> _olderBuffers;

        public long TotalAllocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set;
        }

        public long Allocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set;
        }
        public long InUse
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Initialize(ref ArenaAllocator<TOptions> allocator)
        {
            allocator._nativeAllocator.Initialize(ref allocator._nativeAllocator);            

            allocator.TotalAllocated = 0;
            allocator.Allocated = 0;
            allocator.InUse = 0;

            allocator._preferredSize = allocator._options.InitialArenaSize;
        }

        public void Configure<TConfig>(ref ArenaAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(TOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TOptions)}");

            // This cast will get evicted by the JIT. 
            allocator._options = (TOptions)(object)configuration;
            allocator._nativeAllocator.Configure(ref allocator._nativeAllocator, ref configuration);
        }

        public Pointer Allocate(ref ArenaAllocator<TOptions> allocator, int size)
        {
            if (!allocator._currentBuffer.IsValid || allocator._used + size > allocator._currentBuffer.Size)
            {
                allocator.GrowArena(ref allocator, size);
            }

            Pointer ptr = new Pointer(allocator._ptrCurrent, size);
            allocator._ptrCurrent += ptr.Size;
            allocator._used += ptr.Size;

            allocator.InUse += ptr.Size;
            allocator.TotalAllocated += ptr.Size;

            return ptr;
        }

        private void GrowArena( ref ArenaAllocator<TOptions> allocator, int requestedSize)
        {
            // We could be requesting to allocate less than what the app requested (for big requests) so 
            // we account for that in case it happens. We would override also the MaxArenaSize in those cases
            // and resort to straight memory block allocation
            int newSize = Math.Max(requestedSize, allocator._preferredSize);

            Pointer newBuffer;
            try
            {
                newBuffer = allocator._nativeAllocator.Allocate(ref allocator._nativeAllocator, newSize);
            }
            catch (OutOfMemoryException oom)
                when (oom.Data?.Contains("Recoverable") != true) // this can be raised if the commit charge is low
            {
                // we were too eager with memory allocations?
                newBuffer = allocator._nativeAllocator.Allocate(ref allocator._nativeAllocator, requestedSize);
            }            

            if (allocator._currentBuffer.IsValid)
            {
                if (allocator._olderBuffers == null)
                    allocator._olderBuffers = new List<Pointer>();

                allocator._olderBuffers.Add(allocator._currentBuffer);
            }
            
            allocator._currentBuffer = newBuffer;
            allocator._ptrStart = (byte*)newBuffer.Address;
            allocator._ptrCurrent = (byte*)newBuffer.Address;

            allocator._used = 0;

            allocator.Allocated += newBuffer.Size;
        }

        /// <summary>
        /// It could happen that we have fragmentation, in those cases nothing to be done here. 
        /// Note that this fragmentation will be healed by the call to Reset trying to do this on the fly
        /// is too expensive. Consider to compose this allocator with a PoolAllocator instead if
        /// high fragmentation is expected. 
        /// </summary>
        /// <param name="allocator">The allocator</param>
        /// <param name="ptr">The pointer to release</param>
        public void Release(ref ArenaAllocator<TOptions> allocator, ref Pointer ptr)
        {            
            byte* address = (byte*)ptr.Address;
            if (address >= allocator._ptrStart && address == allocator._ptrCurrent - ptr.Size)
            {
                // since the returned allocation is at the end of the arena, we can move
                // the pointer back
                allocator._used -= ptr.Size;
                allocator._ptrCurrent -= ptr.Size;
            }

            allocator.InUse -= ptr.Size;
            ptr = new Pointer();
        }

        public void Reset(ref ArenaAllocator<TOptions> allocator)
        {
            // Is our strategy requiring us to change the size after having worked with it?
            // The reasons to consider current buffer size and also the used amount of memory is very important to figure out
            // what is the optimal policy for the arena size on the next round.
            int preferredSize = allocator._options.GrowthStrategy.GetPreferredSize(allocator._currentBuffer.Size, allocator.InUse);

            if (allocator._olderBuffers != null)
            {
                // Free old buffers not being used anymore. A single arena ought to be enough for anybody, you know ;) 
                foreach (var unusedBuffer in allocator._olderBuffers)
                {
                    _used += unusedBuffer.Size;

                    Pointer ptr = unusedBuffer;

                    allocator.Allocated -= ptr.Size;
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref ptr);
                }
                allocator._olderBuffers.Clear();
            }

            // If we didnt use more memory than the currently allocated or we are in low memory mode
            if (allocator.InUse <= allocator._currentBuffer.Size || Allocator.LowMemoryFlag.IsRaised())
            {
                // Override with the current setup
                preferredSize = allocator._currentBuffer.Size;
            }

            if (allocator._currentBuffer.IsValid)
            {
                allocator.Allocated -= _currentBuffer.Size;
                allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref _currentBuffer);                
            }                

            // We will always cap the size of the Arena toward the MaxArenaSize.
            if (preferredSize > allocator._options.MaxArenaSize)
                preferredSize = allocator._options.MaxArenaSize;

            allocator._preferredSize = preferredSize;
            allocator._currentBuffer = new Pointer();
            allocator._ptrStart = allocator._ptrCurrent = null;
            allocator._used = 0;

            allocator.TotalAllocated = 0;
            allocator.Allocated = 0;
            allocator.InUse = 0;
        }

        public void Renew(ref ArenaAllocator<TOptions> allocator)
        {
            if (!allocator._currentBuffer.IsValid)
            {
                allocator._currentBuffer = allocator._nativeAllocator.Allocate(ref allocator._nativeAllocator, (int)allocator._preferredSize);
                allocator.Allocated += allocator._currentBuffer.Size;
            }

            if (allocator._olderBuffers != null)
            {
                // Free old buffers not being used anymore. A single arena ought to be enough for anybody, you know ;) 
                foreach (var unusedBuffer in allocator._olderBuffers)
                {
                    _used += unusedBuffer.Size;

                    Pointer ptr = unusedBuffer;

                    allocator.Allocated -= ptr.Size;
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref ptr);
                }

                allocator._olderBuffers.Clear();
            }

            allocator._ptrStart = allocator._ptrCurrent = (byte*)allocator._currentBuffer.Address;
            allocator._used = 0;
            allocator.InUse = 0;
        }

        public void OnAllocate(ref ArenaAllocator<TOptions> allocator, Pointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void OnRelease(ref ArenaAllocator<TOptions> allocator, Pointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void Dispose(ref ArenaAllocator<TOptions> allocator, bool disposing)
        {            
            if (allocator._olderBuffers != null)
            {
                // Free the remanent buffers on local storage from last cycle regrowths
                foreach (var unusedBuffer in allocator._olderBuffers)
                {
                    _used += unusedBuffer.Size;

                    Pointer ptr = unusedBuffer;
                    allocator._nativeAllocator.Release(ref _nativeAllocator, ref ptr);
                }
            }

            if (allocator._currentBuffer.IsValid)
            {
                allocator._nativeAllocator.Release(ref _nativeAllocator, ref _currentBuffer);
            }

            allocator._nativeAllocator.Dispose(ref allocator._nativeAllocator, disposing);
        }
    }
}

