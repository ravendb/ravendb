using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using Sparrow.Global;

namespace Sparrow
{
    public interface IArenaAllocatorOptions : INativeOptions
    {
        int InitialBlockSize { get; }
        int MaxBlockSize { get; }
        IAllocatorComposer<BlockPointer> CreateAllocator();
    }

    public static class ArenaAllocator
    {
        public struct Default : IArenaAllocatorOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int InitialBlockSize => 1 * Constants.Size.Megabyte;
            public int MaxBlockSize => 16 * Constants.Size.Megabyte;
            public IAllocatorComposer<BlockPointer> CreateAllocator() => new BlockAllocator<PoolAllocator<PoolAllocator.Default>>();
        }
    }

    public unsafe struct ArenaAllocator<TOptions> : IAllocator<ArenaAllocator<TOptions>, Pointer>, IAllocator, IDisposable, ILowMemoryHandler<ArenaAllocator<TOptions>>
        where TOptions : struct, IArenaAllocatorOptions
    {
        private TOptions _options;
        private IAllocatorComposer<BlockPointer> _internalAllocator;

        private long _allocated;
        private long _used;

        public void Configure<TConfig>(ref ArenaAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(TOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TOptions)}");

            // This cast will get evicted by the JIT. 
            allocator._options = (TOptions)(object)configuration;
            // PERF: This should be devirtualized. 
            allocator._internalAllocator = allocator._options.CreateAllocator();
        }

        public Pointer Allocate(ref ArenaAllocator<TOptions> allocator, int size)
        {
            throw new NotImplementedException();
        }

        public void Release(ref ArenaAllocator<TOptions> allocator, ref Pointer ptr)
        {
            throw new NotImplementedException();
        }

        public int Allocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Initialize(ref ArenaAllocator<TOptions> allocator)
        {
            allocator._internalAllocator.Initialize(_options);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Allocate(ref ArenaAllocator<TOptions> allocator, int size, ref BlockPointer ptr)
        {
            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ref ArenaAllocator<TOptions> allocator, ref BlockPointer ptr)
        {
        }

        public void Reset(ref ArenaAllocator<TOptions> allocator)
        {
        }

        public void OnAllocate(ref ArenaAllocator<TOptions> allocator, Pointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void OnRelease(ref ArenaAllocator<TOptions> allocator, Pointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void NotifyLowMemory(ref ArenaAllocator<TOptions> allocator)
        {
            // This allocator cannot do anything with this signal.
        }

        public void NotifyLowMemoryOver(ref ArenaAllocator<TOptions> allocator)
        {
            // This allocator cannot do anything with this signal.
        }

        public void Dispose()
        {
        }
    }
}
