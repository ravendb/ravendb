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

        /// <summary>
        /// Whenever the arena cannot allocate anymore, it will regrowth its memory space.
        /// We can control how big that allocation by implementing this method.
        /// </summary>
        /// <param name="allocated">The currently allocated arena size</param>
        /// <param name="used">The memory used from this arena on the current cycle.</param>
        /// <returns>The new size for the arena memory</returns>
        int GetGrowthSize(long allocated, long used);
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
                return (int)allocated;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int GetGrowthSize(long allocated, long used)
            {
                // Always multiply by 2
                return (int)used * 2;
            }
        }

        public struct DoublePowerOf2GrowthStrategy : IArenaGrowthStrategy
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int GetPreferredSize(long allocated, long used)
            {
                return (int)allocated;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public int GetGrowthSize(long allocated, long used)
            {
                return Bits.NextPowerOf2((int)used + 1);              
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

        // Buffers that has been used and cannot be cleaned until a reset happens.
        private List<Pointer> _olderBuffers;

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
            if (_ptrStart == null)
                ThrowInvalidAllocateFromResetWithoutRenew();

            if (allocator._used + size > allocator._currentBuffer.Size)
            {
                allocator.GrowArena(ref allocator, size);
            }

            Pointer ptr = new Pointer(allocator._ptrCurrent, size);
            allocator._ptrCurrent += size;
            allocator._used += size;
            allocator.Allocated += size;

            return ptr;
        }

        private void GrowArena( ref ArenaAllocator<TOptions> allocator, int requestedSize)
        {
            int newSize = allocator._options.GrowthStrategy.GetGrowthSize(allocator._currentBuffer.Size, allocator._used);            
            if (newSize > allocator._options.MaxArenaSize)
                newSize = allocator._options.MaxArenaSize;

            // We could be requesting to allocate less than what the app requested (for big requests) so 
            // we account for that in case it happens. We would override also the MaxArenaSize in those cases
            // and resort to straight memory block allocation
            newSize = Math.Max(requestedSize, newSize);

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
                newSize = requestedSize;           
            }            

            if (allocator._currentBuffer.IsValid)
            {
                if (allocator._olderBuffers == null)
                    allocator._olderBuffers = new List<Pointer>();

                allocator._olderBuffers.Add(allocator._currentBuffer);
            }
            
            allocator._currentBuffer = newBuffer;
            allocator._ptrStart = (byte*)newBuffer.Ptr;
            allocator._ptrCurrent = (byte*)newBuffer.Ptr;

            allocator._used = 0;            
        }

        private static void ThrowInvalidAllocateFromResetWithoutRenew()
        {
            throw new InvalidOperationException("Attempt to allocate from reset arena without calling renew");
        }

        public void Release(ref ArenaAllocator<TOptions> allocator, ref Pointer ptr)
        {
            byte* address = (byte*)ptr.Ptr;
            if (address < allocator._ptrStart || address != allocator._ptrCurrent - ptr.Size)
            {
                // we have fragmentation, so nothing to be done here. 
                // note that this fragmentation will be healed by the call to Reset trying to do this on the fly
                // is too expensive. Consider to compose this allocator with a PoolAllocator instead if
                // high fragmentation is expected. 
                return;
            }

            // since the returned allocation is at the end of the arena, we can just move
            // the pointer back
            allocator._used -= ptr.Size;
            allocator.Allocated -= ptr.Size;
            allocator._ptrCurrent -= ptr.Size;
        }

        // How many bytes has this arena allocated from its memory provider since resets
        public long Allocated
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
            allocator.Renew(ref allocator);
        }

        public void Reset(ref ArenaAllocator<TOptions> allocator)
        {
            // Reset current arena buffer
            allocator._ptrCurrent = allocator._ptrStart;

            // Is our strategy requiring us to change the size after having worked with it?
            long preferredSize = allocator._options.GrowthStrategy.GetPreferredSize(allocator._currentBuffer.Size, allocator._used);

            // Check if there has been new allocations happening in this round.
            if (allocator._olderBuffers == null || allocator._olderBuffers.Count == 0)
            {
                // Given that no new allocation happened, the size is probably too big. 
                if (preferredSize < allocator._currentBuffer.Size && preferredSize > allocator._options.InitialArenaSize * 2)
                {
                    if (allocator._ptrStart != null)
                        allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref _currentBuffer);

                    allocator._currentBuffer = allocator._nativeAllocator.Allocate(ref allocator._nativeAllocator, (int)preferredSize);
                    allocator._ptrCurrent = allocator._ptrStart = null;
                }

                // Nothing else to do here.
                allocator.Allocated = 0;
                allocator._used = 0;
                return;
            }

            // Free old buffers not being used anymore
            foreach (var unusedBuffer in allocator._olderBuffers)
            {                
                _used += unusedBuffer.Size;

                Pointer ptr = unusedBuffer;
                allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref ptr);
            }
            allocator._olderBuffers.Clear();

            // If we didnt use more memory than the currently allocated or we are in low memory mode
            if (allocator._used <= allocator._currentBuffer.Size || Allocator.LowMemoryFlag.IsRaised())            
            {
                // Go on with the current setup
                allocator._used = 0;
                allocator.Allocated = 0;
                return;
            }

            // Given that we used more than the current size of the buffer,
            // We'll likely need more memory in the next round, let us increase the size we hold on to
            if (allocator._ptrStart != null)
                allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref _currentBuffer);

            long newSize = allocator._options.GrowthStrategy.GetPreferredSize(allocator._currentBuffer.Size, allocator._used);
            if (newSize > allocator._options.MaxArenaSize)
                newSize = allocator._options.MaxArenaSize;

            allocator._currentBuffer = allocator._nativeAllocator.Allocate(ref allocator._nativeAllocator, (int)newSize);
            allocator._ptrStart = allocator._ptrCurrent = (byte*)allocator._currentBuffer.Ptr;
            allocator.Allocated = 0;

            allocator._used = 0;
        }

        public void Renew(ref ArenaAllocator<TOptions> allocator)
        {
            if (allocator._ptrStart != null)
                return;

            allocator._currentBuffer = allocator._nativeAllocator.Allocate(ref allocator._nativeAllocator, (int)allocator._currentBuffer.Size);
            allocator._ptrStart = allocator._ptrCurrent = (byte*)allocator._currentBuffer.Ptr;
            allocator.Allocated = 0;
            allocator._used = 0;
        }

        public void OnAllocate(ref ArenaAllocator<TOptions> allocator, Pointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void OnRelease(ref ArenaAllocator<TOptions> allocator, Pointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void Dispose(ref ArenaAllocator<TOptions> allocator)
        {
            // Free old buffers not being used anymore
            foreach (var unusedBuffer in allocator._olderBuffers)
            {
                _used += unusedBuffer.Size;

                Pointer ptr = unusedBuffer;
                allocator._nativeAllocator.Release(ref _nativeAllocator, ref ptr);
            }

            if (_ptrStart != null)
            {
                allocator._nativeAllocator.Release(ref _nativeAllocator, ref _currentBuffer);
            }

            allocator._nativeAllocator.Dispose(ref allocator._nativeAllocator);
        }
    }
}
