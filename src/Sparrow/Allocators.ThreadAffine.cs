using System;
using System.Reflection;
using System.Threading;
using Sparrow.Global;

namespace Sparrow
{
    public enum ThreadAffineWorkload : byte
    {
        Peaceful = 4,
        Default = 16,
        Contended = 64,
        Absurd = 128
    }

    public interface IThreadAffineBlockOptions : INativeOptions, IBlockAllocatorOptions
    {
        bool AcceptOnlyBlocks { get; }

        int ItemsPerLane { get; }
        
        ThreadAffineWorkload Workload { get; }
    }

    public static class ThreadAffineBlockAllocator
    {
        public struct Default : IThreadAffineBlockOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int ItemsPerLane => 4;
            public int BlockSize => 4 * Constants.Size.Kilobyte;
            public bool AcceptOnlyBlocks => false;

            public ThreadAffineWorkload Workload => ThreadAffineWorkload.Default;
        }

        public struct FixedSizeDefault : IThreadAffineBlockOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int ItemsPerLane => 4;
            public int BlockSize => 4 * Constants.Size.Kilobyte;
            public bool AcceptOnlyBlocks => true;

            public ThreadAffineWorkload Workload => ThreadAffineWorkload.Default;
        }

    }

    public unsafe struct ThreadAffineBlockAllocator<TOptions> : IAllocator<ThreadAffineBlockAllocator<TOptions>, Pointer>, ILowMemoryHandler<ThreadAffineBlockAllocator<TOptions>>
            where TOptions : struct, IThreadAffineBlockOptions
    {
        private TOptions _options;
        private NativeAllocator<TOptions> _nativeAllocator;
        private Container[] _container;

        private struct Container
        {
            public IntPtr Block1;
            public IntPtr Block2;
            public IntPtr Block3;
            public IntPtr Block4;
        }

        public long Allocated { get; }

        public void Initialize(ref ThreadAffineBlockAllocator<TOptions> allocator)
        {
            allocator._nativeAllocator.Initialize(ref allocator._nativeAllocator);
            allocator._container = new Container[(int)allocator._options.Workload]; // PERF: This should be a constant.            
        }

        public void Configure<TConfig>(ref ThreadAffineBlockAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(TOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TOptions)}");

            // This cast will get evicted by the JIT. 
            allocator._options = (TOptions)(object)configuration;

            if (allocator._options.ItemsPerLane < 1)
                throw new ArgumentOutOfRangeException($"{nameof(allocator._options.ItemsPerLane)} cannot be smaller than 1.");
            if (allocator._options.ItemsPerLane > 4)
                throw new ArgumentOutOfRangeException($"{nameof(allocator._options.ItemsPerLane)} cannot be bigger than 4.");
        }

        public Pointer Allocate(ref ThreadAffineBlockAllocator<TOptions> allocator, int size)
        {
            if (allocator._options.AcceptOnlyBlocks && size != allocator._options.BlockSize)
                throw new InvalidOperationException(
                    $"This instances of {nameof(ThreadAffineBlockAllocator<TOptions>)} only accepts block size request. Configure the {nameof(TOptions)} to support uncacheable sizes.");

            IntPtr nakedPtr;
            if (size == allocator._options.BlockSize)
            {
                // PERF: Bitwise add should emit a 'and' instruction followed by a constant.
                int threadId = Thread.CurrentThread.ManagedThreadId & ((int)allocator._options.Workload - 1);

                ref Container container = ref allocator._container[threadId];

                nakedPtr = Interlocked.CompareExchange(ref container.Block1, IntPtr.Zero, container.Block1);
                if (nakedPtr != IntPtr.Zero)
                    goto SUCCESS;

                if (allocator._options.ItemsPerLane > 1) // PERF: This check will get evicted
                {
                    nakedPtr = Interlocked.CompareExchange(ref container.Block2, IntPtr.Zero, container.Block2);
                    if (nakedPtr != IntPtr.Zero)
                        goto SUCCESS;
                }

                if (allocator._options.ItemsPerLane > 2) // PERF: This check will get evicted
                {
                    nakedPtr = Interlocked.CompareExchange(ref container.Block3, IntPtr.Zero, container.Block3);
                    if (nakedPtr != IntPtr.Zero)
                        goto SUCCESS;
                }

                if (allocator._options.ItemsPerLane > 3) // PERF: This check will get evicted
                {
                    nakedPtr = Interlocked.CompareExchange(ref container.Block4, IntPtr.Zero, container.Block4);
                    if (nakedPtr != IntPtr.Zero)
                        goto SUCCESS;
                }
            }

            return allocator._nativeAllocator.Allocate(ref allocator._nativeAllocator, size);

            SUCCESS:
            return new Pointer(nakedPtr.ToPointer(), allocator._options.BlockSize);
        }

        public void Release(ref ThreadAffineBlockAllocator<TOptions> allocator, ref Pointer ptr)
        {
            void* nakedPtr = ptr.Ptr;
            if (ptr.Size == allocator._options.BlockSize)
            {
                // PERF: Bitwise add should emit a and instruction followed by a constant.
                int threadId = Thread.CurrentThread.ManagedThreadId & ((int)allocator._options.Workload - 1);

                ref Container container = ref allocator._container[threadId];

                if (Interlocked.CompareExchange(ref container.Block1, (IntPtr)nakedPtr, IntPtr.Zero) == IntPtr.Zero)
                    return;

                // PERF: The items per lane check will get evicted because of constant elimination and therefore the complete code when items is higher. 
                if (allocator._options.ItemsPerLane > 1 && Interlocked.CompareExchange(ref container.Block2, (IntPtr)nakedPtr, IntPtr.Zero) == IntPtr.Zero)
                    return;
                if (allocator._options.ItemsPerLane > 2 && Interlocked.CompareExchange(ref container.Block3, (IntPtr)nakedPtr, IntPtr.Zero) == IntPtr.Zero)
                    return;
                if (allocator._options.ItemsPerLane > 3 && Interlocked.CompareExchange(ref container.Block4, (IntPtr)nakedPtr, IntPtr.Zero) == IntPtr.Zero)
                    return;
            }

            allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref ptr);
        }

        public void Reset(ref ThreadAffineBlockAllocator<TOptions> allocator)
        {
           // When we reset if we are in a low memory condition we will cleanup the pool 
           if (Allocator.LowMemoryFlag.IsRaised())
               CleanupPool(ref allocator);
        }

        public void OnAllocate(ref ThreadAffineBlockAllocator<TOptions> allocator, Pointer ptr) { }
        public void OnRelease(ref ThreadAffineBlockAllocator<TOptions> allocator, Pointer ptr) { }

        public void Dispose(ref ThreadAffineBlockAllocator<TOptions> allocator)
        {
            allocator.CleanupPool(ref allocator);
        }

        private void CleanupPool(ref ThreadAffineBlockAllocator<TOptions> allocator)
        {
            // We move over the whole pool and release what we find. 
            for (int i = 0; i < allocator._container.Length; i++)
            {
                ref Container container = ref allocator._container[i];

                IntPtr ptr = Interlocked.CompareExchange(ref container.Block1, IntPtr.Zero, container.Block1);
                if (ptr != IntPtr.Zero)
                {
                    var localPtr = new Pointer(ptr.ToPointer(), allocator._options.BlockSize);
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref localPtr);
                }

                ptr = Interlocked.CompareExchange(ref container.Block2, IntPtr.Zero, container.Block2);
                if (ptr != IntPtr.Zero)
                {
                    var localPtr = new Pointer(ptr.ToPointer(), allocator._options.BlockSize);
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref localPtr);
                }
                
                ptr = Interlocked.CompareExchange(ref container.Block3, IntPtr.Zero, container.Block3);
                if (ptr != IntPtr.Zero)
                {
                    var localPtr = new Pointer(ptr.ToPointer(), allocator._options.BlockSize);
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref localPtr);
                }

                ptr = Interlocked.CompareExchange(ref container.Block4, IntPtr.Zero, container.Block4);
                if (ptr != IntPtr.Zero)
                {
                    var localPtr = new Pointer(ptr.ToPointer(), allocator._options.BlockSize);
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref localPtr);
                }
            }
        }

        public void NotifyLowMemory(ref ThreadAffineBlockAllocator<TOptions> allocator)
        {
            // We will try to release as much as we can. 
            CleanupPool(ref allocator);
        }

        public void NotifyLowMemoryOver(ref ThreadAffineBlockAllocator<TOptions> allocator) {}
    }
}
