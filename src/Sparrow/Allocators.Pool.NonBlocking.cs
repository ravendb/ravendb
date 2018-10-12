using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Binary;
using Sparrow.Threading;

namespace Sparrow
{
    /// <summary>
    /// The NonBlockingPoolAllocator is a thread safe general pool allocator, which will hold all the memory it can during the process.
    /// It will not keep track of allocations (except when running in validation mode), that means this allocator can leak if used improperly.
    /// The idea is that we will store in concurrent stacks the blocks of memory and use non-blocking APIs to release and allocate from the pool.
    /// </summary>
    /// <typeparam name="TOptions">The options to use for the allocator.</typeparam>
    /// <remarks>The Options object must be properly implemented to achieve performance improvements. (use constants as much as you can on configuration)</remarks>
    public unsafe struct NonBlockingPoolAllocator<TOptions> : IAllocator<NonBlockingPoolAllocator<TOptions>, BlockPointer>, ILowMemoryHandler<NonBlockingPoolAllocator<TOptions>>, IRenewable<NonBlockingPoolAllocator<TOptions>>
        where TOptions : struct, IPoolAllocatorOptions
    {
        private TOptions _options;        

        // PERF: This should be devirtualized.        
        private IAllocatorComposer<Pointer> _internalAllocator;

        private ConcurrentStack<BlockPointer>[] _freed;
        private MultipleUseFlag _illegalOperationsFlag;

        public bool IsThreadSafe => true;

        private long _totalAllocated;
        private long _allocated;
        private long _inUse;

        public long TotalAllocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _totalAllocated; }
        }

        public long Allocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _allocated; }
        }
        public long InUse
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _inUse; }
        }

        public void Initialize(ref NonBlockingPoolAllocator<TOptions> allocator)
        {
            allocator._illegalOperationsFlag = new MultipleUseFlag();
            allocator._totalAllocated = 0;
            allocator._allocated = 0;
            allocator._inUse = 0;
        }

        public void Configure<TConfig>(ref NonBlockingPoolAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(TOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TOptions)}");

            // This cast will get evicted by the JIT.             
            allocator._options = (TOptions)(object)configuration;

            // Initialize the struct pointers structure used to navigate over the allocated memory.    
            allocator._freed = new ConcurrentStack<BlockPointer>[Bits.MostSignificantBit(allocator._options.MaxBlockSize)];
            for (int i = 0; i < allocator._freed.Length; i++)
                allocator._freed[i] = new ConcurrentStack<BlockPointer>();

            allocator._internalAllocator = allocator._options.CreateAllocator();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BlockPointer Allocate(ref NonBlockingPoolAllocator<TOptions> allocator, int size)
        {
            while (allocator._illegalOperationsFlag.IsRaised())
                Thread.Yield();

            // We are effectively disabling the pooling code generation.
            // It is useful for cases where composition is done through type chaining. 
            if (allocator._options.MaxPoolSizeInBytes > 0)
            {
                int vsize = Bits.PowerOf2(Math.Max(sizeof(BlockPointer), size));

                var index = Bits.MostSignificantBit(vsize) - 2; // We use -2 because we are not starting at 0.                                   
                if (index < allocator._freed.Length)
                {
                    if (allocator._freed[index].TryPop(out BlockPointer section))
                    {
                        // Pointer was holding the marker for the next released block instead. 
                        Interlocked.Add(ref allocator._inUse, size);
                        Interlocked.Add(ref allocator._totalAllocated, section.Size);

                        return new BlockPointer(section.Address, section.BlockSize, size);
                    }
                }
            }

            Pointer ptr;
            if (_internalAllocator.IsThreadSafe)
            {
                ptr = _internalAllocator.Allocate(size);
            }
            else
            {
                lock (_internalAllocator)
                {
                    ptr = _internalAllocator.Allocate(size);
                }
            }           

            Interlocked.Add(ref allocator._inUse, size);
            Interlocked.Add(ref allocator._allocated, ptr.Size);
            Interlocked.Add(ref allocator._totalAllocated, ptr.Size);

            return new BlockPointer(ptr.Address, size, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ref NonBlockingPoolAllocator<TOptions> allocator, ref BlockPointer ptr)
        {
            while (allocator._illegalOperationsFlag.IsRaised())
                Thread.Yield();

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

            // Put a copy of the currently released memory block on the stack. 
            allocator._freed[index].Push(ptr);
            Interlocked.Add(ref allocator._inUse, -originalSize);

            ptr = new Pointer(); // Nullify the pointer

            return;

        UnlikelyRelease:
            // This should be an unlikely case, when you are running out of memory or over allocated,
            // all performance guarantees go down the drain. So we dont care if we hit expensive calls
            // that allows us to release some steam at the cost of hitting cold code. 
            // https://github.com/dotnet/coreclr/issues/6024

            Interlocked.Add(ref allocator._inUse, -ptr.Size);
            Interlocked.Add(ref allocator._allocated, -ptr.BlockSize);

            Pointer nakedPtr = new Pointer(ptr.Address, ptr.BlockSize);
            if (_internalAllocator.IsThreadSafe)
            {
                allocator._internalAllocator.Release(ref nakedPtr);
            }
            else
            {
                lock (_internalAllocator)
                {
                    allocator._internalAllocator.Release(ref nakedPtr);
                }
            }
            
            ptr = new Pointer(); // Nullify the pointer
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Renew(ref NonBlockingPoolAllocator<TOptions> allocator)
        {
            try
            {
                allocator._illegalOperationsFlag.Raise();

                if (Allocator.LowMemoryFlag.IsRaised())
                {
                    ReleaseMemoryPool(ref allocator);
                    Interlocked.Exchange(ref allocator._allocated, 0);
                }

                allocator._internalAllocator.Renew();

                Interlocked.Exchange(ref allocator._inUse, 0);
            }
            finally
            {
                allocator._illegalOperationsFlag.Lower();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset(ref NonBlockingPoolAllocator<TOptions> allocator)
        {
            try
            {
                allocator._illegalOperationsFlag.Raise();

                if (allocator._options.HasOwnership)
                    ReleaseMemoryPool(ref allocator);
                else
                    ResetMemoryPool(ref allocator);

                allocator._internalAllocator.Reset();

                Interlocked.Exchange(ref allocator._totalAllocated, 0);
                Interlocked.Exchange(ref allocator._allocated, 0);
                Interlocked.Exchange(ref allocator._inUse, 0);
            }
            finally
            {
                allocator._illegalOperationsFlag.Lower();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnAllocate(ref NonBlockingPoolAllocator<TOptions> allocator, BlockPointer ptr)
        {
            // Nothing to do here.
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnRelease(ref NonBlockingPoolAllocator<TOptions> allocator, BlockPointer ptr)
        {
            // Nothing to do here.
        }

        public void Dispose(ref NonBlockingPoolAllocator<TOptions> allocator, bool disposing)
        {
            if (allocator._options.HasOwnership)
            {
                // We are going to be disposed, we then release all holded memory. 
                allocator.ReleaseMemoryPool(ref allocator);
            }

            allocator._options.ReleaseAllocator(allocator._internalAllocator, disposing);
        }

        private void ResetMemoryPool(ref NonBlockingPoolAllocator<TOptions> allocator)
        {
            // We dont own the memory pool, so we just reset the state and let the owner give us memory again on the next cycle.
            // This is the typical mode of operation when the underlying allocator is able to reuse memory (ex. ArenaAllocator).
            for (int i = 0; i < _freed.Length; i++)
            {
                allocator._freed[i].Clear();
            }
        }

        private void ReleaseMemoryPool(ref NonBlockingPoolAllocator<TOptions> allocator)
        {
            // We own the memory pool, so we have to release all the pointers that we have to the parent allocator.
            // This is the typical mode of operation when the underlying allocator is leaky (ex. NativeAllocator). 
            for (int i = 0; i < allocator._freed.Length; i++)
            {
                var stack = _freed[i];
                while (stack.TryPop(out BlockPointer pointer))
                {
                    // The block is guaranteed to be valid, so we release it to the internal allocator.
                    Pointer currentPtr = new Pointer(pointer.Address, pointer.BlockSize);
                    allocator._internalAllocator.Release(ref currentPtr);
                }
            }
        }

        public void NotifyLowMemory(ref NonBlockingPoolAllocator<TOptions> allocator)
        {
            // We are told that we are low in memory, therefore if we own the memory we will release it.
            if (allocator._options.HasOwnership)
                allocator.ReleaseMemoryPool(ref allocator);

            if (allocator._internalAllocator.IsThreadSafe)
            {
                allocator._internalAllocator.LowMemory();
            }
            else
            {
                lock (allocator._internalAllocator)
                {
                    allocator._internalAllocator.LowMemory();
                }
            }           
        }

        public void NotifyLowMemoryOver(ref NonBlockingPoolAllocator<TOptions> allocator)
        {
            if (allocator._internalAllocator.IsThreadSafe)
            {
                allocator._internalAllocator.LowMemoryOver();
            }
            else
            {
                lock (allocator._internalAllocator)
                {
                    allocator._internalAllocator.LowMemoryOver();
                }
            }          
        }
    }
}
