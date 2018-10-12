using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using Sparrow.Binary;
using Sparrow.Global;

namespace Sparrow
{
    public interface IPoolAllocatorOptions : IAllocatorOptions, IComposableAllocator<Pointer>
    {
        int MaxBlockSize { get; }
        int MaxPoolSizeInBytes { get; }
    }

    public static class PoolAllocator
    {
        public struct Default : IPoolAllocatorOptions, INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int MaxBlockSize => 64 * Constants.Size.Megabyte;
            public int MaxPoolSizeInBytes => 256 * Constants.Size.Megabyte;

            public bool HasOwnership => true;

            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<NativeAllocator<Default>>();
                allocator.Initialize(default(Default));
                return allocator;
            }

            /// <summary>
            /// By default whenever we create an allocator we are going to dispose it too when the time comes.
            /// </summary>
            /// <param name="allocator">the allocator to dispose.</param>
            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                allocator?.Dispose(disposing);
            }
        }

        public struct Static : IPoolAllocatorOptions, INativeOptions
        { 
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int MaxBlockSize => 64 * Constants.Size.Megabyte;
            public int MaxPoolSizeInBytes => 256 * Constants.Size.Megabyte;

            public bool HasOwnership => true;

            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<NativeAllocator<Static>>();
                allocator.Initialize(default(Static));
                return allocator;
            }

            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                // For all uses and purposes the underlying Native Allocator will be finalized as Statics should
                // never deallocate until the process dies. This way we also skip the leak checks. 
                allocator?.Dispose(false);
            }
        }
    }

    /// <summary>
    /// The PoolAllocator will hold all the memory it can during the process. It will not keep track of allocations (except when running in validation mode),
    /// that means this allocator can leak if used improperly. 
    /// </summary>
    /// <typeparam name="TOptions">The options to use for the allocator.</typeparam>
    /// <remarks>The Options object must be properly implemented to achieve performance improvements. (use constants as much as you can on configuration)</remarks>
    public unsafe struct PoolAllocator<TOptions> : IAllocator<PoolAllocator<TOptions>, BlockPointer>, ILowMemoryHandler<PoolAllocator<TOptions>>, IRenewable<PoolAllocator<TOptions>>
        where TOptions : struct, IPoolAllocatorOptions
    {
        private TOptions _options;
        private BlockPointer[] _freed;

        // PERF: This should be devirtualized. 
        private IAllocatorComposer<Pointer> _internalAllocator;

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

        public void Initialize(ref PoolAllocator<TOptions> allocator)
        {
            allocator.TotalAllocated = 0;
            allocator.Allocated = 0;
            allocator.InUse = 0;
        }

        public void Configure<TConfig>(ref PoolAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(TOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TOptions)}");

            // This cast will get evicted by the JIT.             
            allocator._options = (TOptions)(object)configuration;

            // Initialize the struct pointers structure used to navigate over the allocated memory.    
            allocator._freed = new BlockPointer[Bits.MostSignificantBit(allocator._options.MaxBlockSize)];
            allocator._internalAllocator = allocator._options.CreateAllocator();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BlockPointer Allocate(ref PoolAllocator<TOptions> allocator, int size)
        {
            // We are effectively disabling the pooling code generation.
            // It is useful for cases where composition is done through type chaining. 
            if (allocator._options.MaxPoolSizeInBytes > 0)
            {
                int vsize = Bits.PowerOf2(Math.Max(sizeof(BlockPointer), size));

                var index = Bits.MostSignificantBit(vsize) - 2; // We use -2 because we are not starting at 0.                                   
                if (index < allocator._freed.Length && allocator._freed[index].IsValid)
                {
                    // Stack copy of the pointer itself.
                    BlockPointer section = _freed[index];

                    // Pointer was holding the marker for the next released block instead. 
                    allocator._freed[index] = *((BlockPointer*)section.Address);
                    allocator.InUse += size;
                    allocator.TotalAllocated += section.Size;

                    return new BlockPointer(section.Address, section.BlockSize, size);
                }
            }        

            var ptr = _internalAllocator.Allocate(size);
            allocator.InUse += size;
            allocator.Allocated += ptr.Size;
            allocator.TotalAllocated += ptr.Size;

            return new BlockPointer(ptr.Address, size, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ref PoolAllocator<TOptions> allocator, ref BlockPointer ptr)
        {
            // When MaxPoolSizeInBytes is zero, we are effectively disabling the pooling code generation.
            // It is useful for cases where composition is done through type chaining. 
            if (allocator.InUse > allocator._options.MaxPoolSizeInBytes || Allocator.LowMemoryFlag.IsRaised() || allocator._options.MaxPoolSizeInBytes == 0)
                goto UnlikelyRelease;

            int originalSize = ptr.Size;

            int size = ptr.BlockSize;
            if (!Bits.IsPowerOfTwo(size))
                size = Bits.PowerOf2(size) >> 1;
            
            var index = Bits.MostSignificantBit(size) - 2; // We use -2 because we are not starting at 0. 

            // Retaining chunks bigger than the max chunk size could clutter the allocator, so we reroute it to the backing allocator.
            if (index < 0 || index >= allocator._freed.Length)
                goto UnlikelyRelease;

            var section = allocator._freed[index];
            if (section.IsValid)
            {
                // Copy the section pointer that is already freed to the current memory. 
                *(BlockPointer*)ptr.Address = section;
            }
            else
            {
                // Put a copy of the currently released memory block on the front. 
                *(BlockPointer*)ptr.Address = new BlockPointer();
            }

            allocator._freed[index] = ptr;
            allocator.InUse -= originalSize;

            ptr = new Pointer(); // Nullify the pointer

            return;

        UnlikelyRelease:
            // This should be an unlikely case, when you are running out of memory or over allocated,
            // all performance guarantees go down the drain. So we dont care if we hit expensive calls
            // that allows us to release some steam at the cost of hitting cold code. 
            // https://github.com/dotnet/coreclr/issues/6024

            allocator.InUse -= ptr.Size;
            allocator.Allocated -= ptr.BlockSize;

            Pointer nakedPtr = new Pointer(ptr.Address, ptr.BlockSize);
            allocator._internalAllocator.Release(ref nakedPtr);

            ptr = new Pointer(); // Nullify the pointer
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Renew(ref PoolAllocator<TOptions> allocator)
        {
            if (Allocator.LowMemoryFlag.IsRaised())
            {
                ReleaseMemoryPool(ref allocator);
                allocator.Allocated = 0;
            }
                
            _internalAllocator.Renew();
            allocator.InUse = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset(ref PoolAllocator<TOptions> allocator)
        {
            if (allocator._options.HasOwnership)
                ReleaseMemoryPool(ref allocator);
            else
                ResetMemoryPool(ref allocator);

            _internalAllocator.Reset();

            allocator.TotalAllocated = 0;
            allocator.Allocated = 0;
            allocator.InUse = 0;
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

        public void Dispose(ref PoolAllocator<TOptions> allocator, bool disposing)
        {
            if (allocator._options.HasOwnership)
                // We are going to be disposed, we then release all holded memory. 
                allocator.ReleaseMemoryPool(ref allocator);

            allocator._options.ReleaseAllocator(allocator._internalAllocator, disposing);
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
                    section = *(BlockPointer*)current.Address;

                    // The block is guaranteed to be valid, so we release it to the internal allocator.
                    Pointer currentPtr = new Pointer(current.Address, current.BlockSize);
                    allocator._internalAllocator.Release(ref currentPtr);
                }
            }
        }

        public void NotifyLowMemory(ref PoolAllocator<TOptions> allocator)
        {
            // We are told that we are low in memory, therefore if we own the memory we will release it.
            if (allocator._options.HasOwnership)
                allocator.ReleaseMemoryPool(ref allocator);

            allocator._internalAllocator.LowMemory();
        }

        public void NotifyLowMemoryOver(ref PoolAllocator<TOptions> allocator)
        {
            allocator._internalAllocator.LowMemoryOver();
        }

        public bool IsThreadSafe => false;
    }
}
