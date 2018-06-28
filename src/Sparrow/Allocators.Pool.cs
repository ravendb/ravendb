using System;
using Sparrow.Binary;

namespace Sparrow
{
    public interface IPoolAllocatorOptions : INativeBlockOptions
    {

    }

    public static class PoolAllocator
    {
        internal unsafe struct FreeSection
        {
            public FreeSection* Previous;
            public int SizeInBytes;
        }
    }

    public unsafe struct PoolBlockAllocator<TOptions> : IAllocator<PoolBlockAllocator<TOptions>, BlockPointer>, IAllocator, IDisposable, ILowMemoryHandler<PoolBlockAllocator<TOptions>>, IRenewable<PoolBlockAllocator<TOptions>>
        where TOptions : struct, IPoolAllocatorOptions
    {
        private TOptions _options;

        private int _allocated;
        private int _used;

        private PoolAllocator.FreeSection*[] _freed;


        public int Allocated => _allocated;
        public int Used => _used;

        public void Initialize(ref PoolBlockAllocator<TOptions> allocator)
        {
            // Initialize the struct pointers structure used to navigate over the allocated memory.
            allocator._freed = new PoolAllocator.FreeSection*[32];
        }

        public void Configure<TConfig>(ref PoolBlockAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            throw new NotImplementedException();
        }

        public BlockPointer Allocate(ref PoolBlockAllocator<TOptions> allocator, int size)
        {
            //if (allocator.__ptrStart == null)
            //    ThrowInvalidAllocateFromResetWithoutRenew();

            if (size < sizeof(PoolAllocator.FreeSection))
                size = sizeof(PoolAllocator.FreeSection);

            size = Bits.NextPowerOf2(size);

            var index = Bits.MostSignificantBit(size) - 1;
            if (_freed[index] != null)
            {
                var section = _freed[index];
                _freed[index] = section->Previous;

            }

            throw new NotImplementedException();
        }

        public void Release(ref PoolBlockAllocator<TOptions> allocator, ref BlockPointer ptr)
        {
            throw new NotImplementedException();
        }


        private static void ThrowInvalidAllocateFromResetWithoutRenew()
        {
            throw new InvalidOperationException("Attempt to allocate from reset arena without calling renew");
        }

        private void ThrowAlreadyDisposedException()
        {
            throw new ObjectDisposedException("This ArenaMemoryAllocator is already disposed");
        }

        public void Renew(ref PoolBlockAllocator<TOptions> allocator)
        {
            throw new NotImplementedException();
        }

        public void Reset(ref PoolBlockAllocator<TOptions> allocator)
        {
            throw new NotImplementedException();
        }

        public void OnAllocate(ref PoolBlockAllocator<TOptions> allocator, BlockPointer ptr)
        {
            throw new NotImplementedException();
        }

        public void OnRelease(ref PoolBlockAllocator<TOptions> allocator, BlockPointer ptr)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public void NotifyLowMemory(ref PoolBlockAllocator<TOptions> allocator)
        {
            throw new NotImplementedException();
        }

        public void NotifyLowMemoryOver(ref PoolBlockAllocator<TOptions> allocator)
        {
            throw new NotImplementedException();
        }
    }


}
