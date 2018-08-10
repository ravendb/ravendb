using System;
using System.Reflection;
using System.Runtime.CompilerServices;
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

    public interface IFixedSizeThreadAffinePoolOptions : IFixedSizeAllocatorOptions, IComposableAllocator<Pointer>
    {        
        bool AcceptOnlyBlocks { get; }
        int ItemsPerLane { get; }
        
        ThreadAffineWorkload Workload { get; }
    }

    public static class FixedSizeThreadAffinePoolAllocator
    {
        public struct Default : IFixedSizeThreadAffinePoolOptions, INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int BlockSize => 1 * Constants.Size.Megabyte;
            public int ItemsPerLane => 4;

            public bool AcceptOnlyBlocks => true;

            public ThreadAffineWorkload Workload => ThreadAffineWorkload.Default;

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
                allocator.Dispose(disposing);
            }
        }

        public struct Static : IFixedSizeThreadAffinePoolOptions, INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int BlockSize => 1 * Constants.Size.Megabyte;
            public int ItemsPerLane => 4;

            public bool AcceptOnlyBlocks => true;

            public ThreadAffineWorkload Workload => ThreadAffineWorkload.Default;

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
                allocator.Dispose(false);
            }
        }

        public struct DefaultAcceptArbitratySize : IFixedSizeThreadAffinePoolOptions, INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int BlockSize => 1 * Constants.Size.Megabyte;
            public int ItemsPerLane => 4;

            public bool AcceptOnlyBlocks => false;

            public ThreadAffineWorkload Workload => ThreadAffineWorkload.Default;

            public bool HasOwnership => true;
            public IAllocatorComposer<Pointer> CreateAllocator()
            {
                var allocator = new Allocator<NativeAllocator<DefaultAcceptArbitratySize>>();
                allocator.Initialize(default(DefaultAcceptArbitratySize));
                return allocator;
            }

            /// <summary>
            /// By default whenever we create an allocator we are going to dispose it too when the time comes.
            /// </summary>
            /// <param name="allocator">the allocator to dispose.</param>
            public void ReleaseAllocator(IAllocatorComposer<Pointer> allocator, bool disposing)
            {
                allocator.Dispose(disposing);
            }
        }
    }

    public unsafe struct FixedSizeThreadAffinePoolAllocator<TOptions> : IAllocator<FixedSizeThreadAffinePoolAllocator<TOptions>, Pointer>, ILowMemoryHandler<FixedSizeThreadAffinePoolAllocator<TOptions>>, IRenewable<FixedSizeThreadAffinePoolAllocator<TOptions>>
            where TOptions : struct, IFixedSizeThreadAffinePoolOptions
    {
        private TOptions _options;

        // PERF: This should be devirtualized. 
        private IAllocatorComposer<Pointer> _internalAllocator;

        private Container[] _container;

        private struct Container
        {
            public IntPtr Block1;
            public IntPtr Block2;
            public IntPtr Block3;
            public IntPtr Block4;
        }

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

        public void Initialize(ref FixedSizeThreadAffinePoolAllocator<TOptions> allocator)
        {
            allocator._container = new Container[(int)allocator._options.Workload]; // PERF: This should be a constant.       

            allocator.TotalAllocated = 0;
            allocator.Allocated = 0;
            allocator.InUse = 0;
        }

        public void Configure<TConfig>(ref FixedSizeThreadAffinePoolAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(TOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TOptions)}");

            // This cast will get evicted by the JIT. 
            allocator._options = (TOptions)(object)configuration;
            allocator._internalAllocator = allocator._options.CreateAllocator();

            if (allocator._options.ItemsPerLane < 1)
                throw new ArgumentOutOfRangeException($"{nameof(allocator._options.ItemsPerLane)} cannot be smaller than 1.");
            if (allocator._options.ItemsPerLane > 4)
                throw new ArgumentOutOfRangeException($"{nameof(allocator._options.ItemsPerLane)} cannot be bigger than 4.");            
        }

        public Pointer Allocate(ref FixedSizeThreadAffinePoolAllocator<TOptions> allocator, int size)
        {
            if (allocator._options.AcceptOnlyBlocks && size != allocator._options.BlockSize)
                throw new InvalidOperationException(
                    $"This instances of {nameof(FixedSizeThreadAffinePoolAllocator<TOptions>)} only accepts block size request. Configure the {nameof(TOptions)} to support uncacheable sizes.");

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

            Pointer ptr = allocator._internalAllocator.Allocate(size);
            allocator.InUse += ptr.Size;
            allocator.Allocated += ptr.Size;
            allocator.TotalAllocated += ptr.Size;
            return ptr;

            SUCCESS:

            allocator.InUse += allocator._options.BlockSize;
            allocator.TotalAllocated += allocator._options.BlockSize;
            return new Pointer(nakedPtr.ToPointer(), allocator._options.BlockSize);
        }

        public void Release(ref FixedSizeThreadAffinePoolAllocator<TOptions> allocator, ref Pointer ptr)
        {
            void* nakedPtr = ptr.Ptr;
            if (ptr.Size != allocator._options.BlockSize || Allocator.LowMemoryFlag.IsRaised())
                goto UnlikelyRelease;

#if VALIDATE
            // We are storing the pointer itself to return it in the future, therefore we have to 
            // clear the generation.
            ptr.Generation = 0;
#endif

            // PERF: Bitwise add should emit a and instruction followed by a constant.
            int threadId = Thread.CurrentThread.ManagedThreadId & ((int)allocator._options.Workload - 1);

            ref Container container = ref allocator._container[threadId];
            if (Interlocked.CompareExchange(ref container.Block1, (IntPtr)nakedPtr, IntPtr.Zero) == IntPtr.Zero)
                goto Success;

            // PERF: The items per lane check will get evicted because of constant elimination and therefore the complete code when items is higher. 
            if (allocator._options.ItemsPerLane > 1 && Interlocked.CompareExchange(ref container.Block2, (IntPtr)nakedPtr, IntPtr.Zero) == IntPtr.Zero)
                goto Success;
            if (allocator._options.ItemsPerLane > 2 && Interlocked.CompareExchange(ref container.Block3, (IntPtr)nakedPtr, IntPtr.Zero) == IntPtr.Zero)
                goto Success;
            if (allocator._options.ItemsPerLane > 3 && Interlocked.CompareExchange(ref container.Block4, (IntPtr)nakedPtr, IntPtr.Zero) == IntPtr.Zero)
                goto Success;

            goto UnlikelyRelease;

        Success:
            allocator.InUse -= ptr.Size;
            ptr = new Pointer(); // Nullify the pointer
            return;

        UnlikelyRelease:

            allocator.InUse -= ptr.Size;
            allocator.Allocated -= ptr.Size;
            allocator._internalAllocator.Release(ref ptr);
        }

        public void Reset(ref FixedSizeThreadAffinePoolAllocator<TOptions> allocator)
        {
            CleanupPool(ref allocator);

            allocator._internalAllocator.Reset();

            allocator.TotalAllocated = 0;
            allocator.Allocated = 0;
            allocator.InUse = 0;
        }

        public void Renew(ref FixedSizeThreadAffinePoolAllocator<TOptions> allocator)
        {
            if (Allocator.LowMemoryFlag.IsRaised())
            {
                CleanupPool(ref allocator);
                allocator.Allocated = 0;
            }                

            allocator._internalAllocator.Renew();
            allocator.InUse = 0;
        }

        public void OnAllocate(ref FixedSizeThreadAffinePoolAllocator<TOptions> allocator, Pointer ptr) { }
        public void OnRelease(ref FixedSizeThreadAffinePoolAllocator<TOptions> allocator, Pointer ptr) { }

        public void Dispose(ref FixedSizeThreadAffinePoolAllocator<TOptions> allocator, bool disposing)
        {
            if (allocator._options.HasOwnership)
            {
                // We are going to be disposed, we then release all holded memory. 
                allocator.CleanupPool(ref allocator);
            }

            allocator._options.ReleaseAllocator(allocator._internalAllocator, disposing);
        }

        private void CleanupPool(ref FixedSizeThreadAffinePoolAllocator<TOptions> allocator)
        {
            // We move over the whole pool and release what we find. 
            for (int i = 0; i < allocator._container.Length; i++)
            {
                ref Container container = ref allocator._container[i];

                IntPtr ptr = Interlocked.CompareExchange(ref container.Block1, IntPtr.Zero, container.Block1);
                if (ptr != IntPtr.Zero)
                {
                    var localPtr = new Pointer(ptr.ToPointer(), allocator._options.BlockSize);
                    allocator._internalAllocator.Release(ref localPtr);
                }

                ptr = Interlocked.CompareExchange(ref container.Block2, IntPtr.Zero, container.Block2);
                if (ptr != IntPtr.Zero)
                {
                    var localPtr = new Pointer(ptr.ToPointer(), allocator._options.BlockSize);
                    allocator._internalAllocator.Release(ref localPtr);
                }
                
                ptr = Interlocked.CompareExchange(ref container.Block3, IntPtr.Zero, container.Block3);
                if (ptr != IntPtr.Zero)
                {
                    var localPtr = new Pointer(ptr.ToPointer(), allocator._options.BlockSize);
                    allocator._internalAllocator.Release(ref localPtr);
                }

                ptr = Interlocked.CompareExchange(ref container.Block4, IntPtr.Zero, container.Block4);
                if (ptr != IntPtr.Zero)
                {
                    var localPtr = new Pointer(ptr.ToPointer(), allocator._options.BlockSize);
                    allocator._internalAllocator.Release(ref localPtr);
                }
            }
        }

        public void NotifyLowMemory(ref FixedSizeThreadAffinePoolAllocator<TOptions> allocator)
        {
            // We will try to release as much as we can. 
            CleanupPool(ref allocator);

            _internalAllocator.LowMemory();
        }

        public void NotifyLowMemoryOver(ref FixedSizeThreadAffinePoolAllocator<TOptions> allocator)
        {
            _internalAllocator.LowMemoryOver();
        }
    }
}
