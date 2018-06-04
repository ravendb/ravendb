using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Sparrow.Global;

namespace Sparrow
{
    public interface IPoolAllocatorOptions
    {
        int MaxChunkSize { get; }
        int MaxPoolMemoryInBytes { get; }

        bool HasOwnership { get; }
        IAllocatorComposer<Pointer> CreateAllocator();
    }

    public static class PoolAllocator
    {
        public struct Default : IPoolAllocatorOptions, INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int MaxChunkSize => 64 * Constants.Size.Megabyte;
            public int MaxPoolMemoryInBytes => 256 * Constants.Size.Megabyte;

            public bool HasOwnership => true;

            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<NativeAllocator<Default>>();
                allocator.Initialize(default(Default));
                return allocator;
            }
        }
    }

    /// <summary>
    /// The PoolAllocator will hold all the memory it can during the process. It will not keep track of allocations (except when running in validation mode),
    /// that means this allocator can leak if used improperly. 
    /// </summary>
    /// <typeparam name="TOptions">The options to use for the allocator.</typeparam>
    /// <remarks>The Options object must be properly implemented to achieve performance improvements. (use constants as much as you can on configuration)</remarks>
    public unsafe struct PoolAllocator<TOptions> : IAllocator<PoolAllocator<TOptions>, BlockPointer>, IRenewable<PoolAllocator<TOptions>>
        where TOptions : struct, IPoolAllocatorOptions
    {
        private TOptions _options;
        private BlockPointer[] _freed;

        // PERF: This should be devirtualized. 
        private IAllocatorComposer<Pointer> _internalAllocator;

        public long Allocated { get; private set; }
        public long Used { get; private set; }

        public void Initialize(ref PoolAllocator<TOptions> allocator)
        {
            // Initialize the struct pointers structure used to navigate over the allocated memory.          
            allocator.Allocated = 0;
            allocator.Used = 0;
        }

        public void Configure<TConfig>(ref PoolAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(TOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TOptions)}");

            // This cast will get evicted by the JIT.             
            allocator._options = (TOptions)(object)configuration;
            allocator._freed = new BlockPointer[Bits.MostSignificantBit(allocator._options.MaxChunkSize)];
            allocator._internalAllocator = allocator._options.CreateAllocator();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BlockPointer Allocate(ref PoolAllocator<TOptions> allocator, int size)
        {
            int vsize = Bits.NextPowerOf2(Math.Max(sizeof(BlockPointer), size));

            var index = Bits.MostSignificantBit(vsize) - 1;
            if (index < allocator._freed.Length && allocator._freed[index].IsValid)
            {
                // Stack copy of the pointer itself.
                BlockPointer section = _freed[index];

                // Pointer was holding the marker for the next released block instead. 
                allocator._freed[index] = *((BlockPointer*)section.Ptr);
                allocator.Used += section.Size;

                return section;
            }

            allocator.Used += size;
            allocator.Allocated += vsize;

            var ptr = _internalAllocator.Allocate(vsize);
            return new BlockPointer(ptr.Ptr, size, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ref PoolAllocator<TOptions> allocator, ref BlockPointer ptr)
        {
            if (allocator.Used > allocator._options.MaxPoolMemoryInBytes || Allocator.LowMemoryFlag.IsRaised())
                goto UnlikelyRelease;

            int originalSize = ptr.Size;

            int size = Bits.NextPowerOf2(ptr.BlockSize);
            var index = Bits.MostSignificantBit(size) - 1;

            // Retaining chunks bigger than the max chunk size could clutter the allocator, so we reroute it to the backing allocator.
            if (index >= allocator._freed.Length)
                goto UnlikelyRelease;

            var section = allocator._freed[index];
            if (section.IsValid)
            {
                // Copy the section pointer that is already freed to the current memory. 
                *(BlockPointer*)ptr.Ptr = section;
            }

            // Put a copy of the currently released memory block on the front. 
            allocator._freed[index] = ptr;

            allocator.Used -= originalSize;

            return;

        UnlikelyRelease:
            // This should be an unlikely case, when you are running out of memory or over allocated,
            // all performance guarantees go down the drain. So we dont care if we hit expensive calls
            // that allows us to release some steam at the cost of hitting cold code. 
            // https://github.com/dotnet/coreclr/issues/6024

            allocator.Used -= ptr.Size;

            Pointer nakedPtr = new Pointer(ptr.Ptr, ptr.BlockSize);
            allocator._internalAllocator.Release(ref nakedPtr);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Renew(ref PoolAllocator<TOptions> allocator)
        {
            if (Allocator.LowMemoryFlag.IsRaised())
                ReleaseMemoryPool(ref allocator);

            _internalAllocator.Renew();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset(ref PoolAllocator<TOptions> allocator)
        {
            if (allocator._options.HasOwnership)
                ReleaseMemoryPool(ref allocator);
            else
                ResetMemoryPool(ref allocator);

            _internalAllocator.Reset();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnAllocate(ref PoolAllocator<TOptions> allocator, BlockPointer ptr)
        {
            // Nothing to do here.
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnRelease(ref PoolAllocator<TOptions> allocator, BlockPointer ptr)
        {
            // Nothing to do here.
        }

        public void Dispose(ref PoolAllocator<TOptions> allocator)
        {
            if (allocator._options.HasOwnership)
                // We are going to be disposed, we then release all holded memory. 
                allocator.ReleaseMemoryPool(ref allocator);

            allocator._internalAllocator.Dispose();
        }

        private void ResetMemoryPool(ref PoolAllocator<TOptions> allocator)
        {
            // We dont own the memory pool, so we just reset the state and let the owner give us memory again on the next cycle.
            // This is the typical mode of operation when the underlying allocator is able to reuse memory (ex. ArenaAllocator).
            for (int i = 0; i < _freed.Length; i++)
            {
                allocator._freed[i] = new BlockPointer();
            }
        }

        private void ReleaseMemoryPool(ref PoolAllocator<TOptions> allocator)
        {
            // We own the memory pool, so we have to release all the pointers that we have to the parent allocator.
            // This is the typical mode of operation when the underlying allocator is leaky (ex. NativeAllocator). 
            for (int i = 0; i < _freed.Length; i++)
            {
                ref var section = ref _freed[i];
                while (section.IsValid)
                {
                    BlockPointer current = section;

                    // Copy the pointer found on the first memory bytes of the section. 
                    section = *(BlockPointer*)current.Ptr;

                    // The block is guaranteed to be valid, so we release it to the internal allocator.
                    Pointer currentPtr = new Pointer(current.Ptr, current.BlockSize);
                    allocator._internalAllocator.Release(ref currentPtr);
                }
            }
        }
    }
}
