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

    public interface IThreadAffineBlockOptions : INativeBlockOptions
    {
        int BlockSize { get; }
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
            public ThreadAffineWorkload Workload => ThreadAffineWorkload.Default;
        }
    }

    public unsafe struct ThreadAffineBlockAllocator<TOptions> : IAllocator<ThreadAffineBlockAllocator<TOptions>, BlockPointer>, IAllocator, IDisposable, ILowMemoryHandler<ThreadAffineBlockAllocator<TOptions>>
            where TOptions : struct, IThreadAffineBlockOptions
    {
        private TOptions _options;
        private NativeBlockAllocator<TOptions> _nativeAllocator;
        private Container[] _container;

        private struct Container
        {
            public IntPtr Block1;
            public IntPtr Block2;
            public IntPtr Block3;
            public IntPtr Block4;
        }

        public int Allocated { get; }

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

        public BlockPointer Allocate(ref ThreadAffineBlockAllocator<TOptions> allocator, int size)
        {
            if (size < allocator._options.BlockSize)
            {
                // PERF: Bitwise add should emit a 'and' instruction followed by a constant.
                int threadId = Thread.CurrentThread.ManagedThreadId & ((int)allocator._options.Workload - 1);

                ref Container container = ref allocator._container[threadId];

                BlockPointer.Header* header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container.Block1, IntPtr.Zero, container.Block1);
                if (header != null)
                    return new BlockPointer(header);

                if (allocator._options.ItemsPerLane > 1) // PERF: This check will get evicted
                {
                    header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container.Block2, IntPtr.Zero, container.Block2);
                    if (header != null)
                        return new BlockPointer(header);
                }

                if (allocator._options.ItemsPerLane > 2) // PERF: This check will get evicted
                {
                    header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container.Block3, IntPtr.Zero, container.Block3);
                    if (header != null)
                        return new BlockPointer(header);
                }

                if (allocator._options.ItemsPerLane > 3) // PERF: This check will get evicted
                {
                    header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container.Block4, IntPtr.Zero, container.Block4);
                    if (header != null)
                        return new BlockPointer(header);
                }
            }

            return allocator._nativeAllocator.Allocate(ref allocator._nativeAllocator, size);
        }

        public void Release(ref ThreadAffineBlockAllocator<TOptions> allocator, ref BlockPointer ptr)
        {
            BlockPointer.Header* header = ptr._header;
            if (header->Size < allocator._options.BlockSize)
            {
                // PERF: Bitwise add should emit a and instruction followed by a constant.
                int threadId = Thread.CurrentThread.ManagedThreadId & ((int)allocator._options.Workload - 1);

                ref Container container = ref allocator._container[threadId];

                if (Interlocked.CompareExchange(ref container.Block1, (IntPtr)header, IntPtr.Zero) == IntPtr.Zero)
                    return;

                // PERF: The items per lane check will get evicted because of constant elimination and therefore the complete code when items is higher. 
                if (allocator._options.ItemsPerLane > 1 && Interlocked.CompareExchange(ref container.Block2, (IntPtr)header, IntPtr.Zero) == IntPtr.Zero)
                    return;
                if (allocator._options.ItemsPerLane > 2 && Interlocked.CompareExchange(ref container.Block3, (IntPtr)header, IntPtr.Zero) == IntPtr.Zero)
                    return;
                if (allocator._options.ItemsPerLane > 3 && Interlocked.CompareExchange(ref container.Block4, (IntPtr)header, IntPtr.Zero) == IntPtr.Zero)
                    return;
            }

            allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref ptr);
        }

        public void Reset(ref ThreadAffineBlockAllocator<TOptions> allocator)
        {
            throw new NotSupportedException($"{nameof(ThreadAffineBlockAllocator<TOptions>)} does not support '.{nameof(Reset)}()'");
        }

        public void OnAllocate(ref ThreadAffineBlockAllocator<TOptions> allocator, BlockPointer ptr) { }
        public void OnRelease(ref ThreadAffineBlockAllocator<TOptions> allocator, BlockPointer ptr) { }

        public void Dispose()
        {
            CleanupPool(ref this);
        }

        public void NotifyLowMemory(ref ThreadAffineBlockAllocator<TOptions> allocator)
        {
            CleanupPool(ref allocator);
        }

        private void CleanupPool(ref ThreadAffineBlockAllocator<TOptions> allocator)
        {
            // We move over the whole pool and release what we find. 
            for (int i = 0; i < allocator._container.Length; i++)
            {
                ref Container container = ref allocator._container[i];

                BlockPointer ptr;
                BlockPointer.Header* header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container.Block1, IntPtr.Zero, container.Block1);
                if (header != null)
                {
                    ptr = new BlockPointer(header);
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref ptr);
                }

                header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container.Block2, IntPtr.Zero, container.Block2);
                if (header != null)
                {
                    ptr = new BlockPointer(header);
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref ptr);
                }

                header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container.Block3, IntPtr.Zero, container.Block3);
                if (header != null)
                {
                    ptr = new BlockPointer(header);
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref ptr);
                }

                header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container.Block4, IntPtr.Zero, container.Block4);
                if (header != null)
                {
                    ptr = new BlockPointer(header);
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, ref ptr);
                }
            }
        }

        public void NotifyLowMemoryOver(ref ThreadAffineBlockAllocator<TOptions> allocator)
        {
            // Nothing to do here. 
        }
    }
}
