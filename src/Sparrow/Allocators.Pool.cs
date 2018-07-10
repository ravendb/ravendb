using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Sparrow.Global;

namespace Sparrow
{
    public interface IPoolAllocatorOptions : INativeOptions
    {
        int MaxPoolMemoryInBytes { get; }
        IAllocatorComposer<Pointer> CreateAllocator();
    }

    public static class PoolAllocator
    {
        public struct Default : IPoolAllocatorOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int MaxPoolMemoryInBytes => 256 * Constants.Size.Megabyte;

            public IAllocatorComposer<Pointer> CreateAllocator() => new Allocator<NativeAllocator<Default>>();
        }
    }

    /// <summary>
    /// The PoolAllocator will hold all the memory it can during the process. It will not keep track of allocations (except when running in validation mode),
    /// that means this allocator can leak if used improperly. 
    /// </summary>
    /// <typeparam name="TOptions">The options to use for the allocator.</typeparam>
    /// <remarks>The Options object must be properly implemented to achieve performance improvements. (use constants as much as you can)</remarks>
    public unsafe struct PoolAllocator<TOptions> : IAllocator<PoolAllocator<TOptions>, Pointer>, IAllocator, IDisposable, ILowMemoryHandler<PoolAllocator<TOptions>>, IRenewable<PoolAllocator<TOptions>>
        where TOptions : struct, IPoolAllocatorOptions
    {
        private TOptions _options;
        private bool _isMemoryLow;
        private Pointer[] _freed;
        private IAllocatorComposer<Pointer> _internalAllocator;

        public int Allocated { get; private set; }    
        public int Used { get; private set; }

        public void Initialize(ref PoolAllocator<TOptions> allocator)
        {
            // Initialize the struct pointers structure used to navigate over the allocated memory.
            allocator._freed = new Pointer[27];
            allocator.Allocated = 0;
            allocator.Used = 0;
        }

        public void Configure<TConfig>(ref PoolAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(TOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TOptions)}");

            // This cast will get evicted by the JIT. 
            allocator._options = (TOptions)(object)configuration;
            // PERF: This should be devirtualized. 
            allocator._internalAllocator = allocator._options.CreateAllocator();
            allocator._internalAllocator.Initialize(allocator._options);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Pointer Allocate(ref PoolAllocator<TOptions> allocator, int size)
        {
            int vsize = Bits.NextPowerOf2(size);

            var index = Bits.MostSignificantBit(vsize) - 1;
            if (_freed[index].IsValid)
            {   
                // Stack copy of the pointer itself.
                Pointer section = _freed[index];

                // Pointer was holding the marker for the next released block instead. 
                _freed[index] = *((Pointer*)section.Ptr);
                allocator.Used += section.Size;

                return section;
            }

            allocator.Used += size;
            allocator.Allocated += vsize;

            var ptr =  _internalAllocator.Allocate(vsize);
            return new Pointer(ptr.Ptr, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ref PoolAllocator<TOptions> allocator, ref Pointer ptr)
        {
            if (allocator._isMemoryLow || allocator.Used > allocator._options.MaxPoolMemoryInBytes)
                goto UnlikelyRelease;

            int originalSize = ptr.Size;

            int size = Bits.NextPowerOf2(ptr.Size);
            var index = Bits.MostSignificantBit(size) - 1;
            
            // Allocating more than 2^26 (100MB should not be pooled, chunks are big enough to clutter the allocator)
            if (index >= 27) 
                goto UnlikelyRelease;

            var section = _freed[index];
            if (section.IsValid)
            {
                // Copy the section pointer that is already freed to the current memory. 
                *(Pointer*)ptr.Ptr = section;
            }
            
            // Put a copy of the currently released memory block on the front. 
            _freed[index] = ptr;

            allocator.Used -= originalSize;

            return;

        UnlikelyRelease:
            // This should be an unlikely case, when you are running out of memory or over allocated,
            // all performance guarantees go down the drain. So we dont care if we hit expensive calls
            // that allows us to release some steam at the cost of hitting cold code. 
            // https://github.com/dotnet/coreclr/issues/6024

            allocator.Used -= ptr.Size;
            allocator._internalAllocator.Release(ref ptr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Renew(ref PoolAllocator<TOptions> allocator)
        {
            if (!allocator._isMemoryLow)
                return;

            // We are low on memory, we will release the whole pool.
            ReleasePoolMemory(ref allocator);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset(ref PoolAllocator<TOptions> allocator)
        {
            if (!allocator._isMemoryLow)
                return;

            // We are low on memory, we will release the whole pool.
            ReleasePoolMemory(ref allocator);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnAllocate(ref PoolAllocator<TOptions> allocator, Pointer ptr)
        {
            // Nothing to do here.
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnRelease(ref PoolAllocator<TOptions> allocator, Pointer ptr)
        {
            // Nothing to do here.
        }

        public void Dispose()
        {
            // We are going to be disposed, we then release all holded memory. 
            ReleasePoolMemory(ref this);
        }

        public void NotifyLowMemory(ref PoolAllocator<TOptions> allocator)
        {
            allocator._isMemoryLow = true;
        }

        public void NotifyLowMemoryOver(ref PoolAllocator<TOptions> allocator)
        {
            allocator._isMemoryLow = false;
        }

        private void ReleasePoolMemory(ref PoolAllocator<TOptions> allocator)
        {
            for (int i = 0; i < 32; i++)
            {
                ref var section = ref _freed[i];
                while (section.IsValid)
                {
                    Pointer current = section;

                    // Copy the pointer found on the first memory bytes of the section. 
                    section = *(Pointer*)current.Ptr;

                    // The block is guaranteed to be valid, so we release it to the internal allocator.
                    allocator._internalAllocator.Release(ref current);
                }
            }
        }
    }
}
