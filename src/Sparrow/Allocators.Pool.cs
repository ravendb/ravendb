using System;
using System.Reflection;
using System.Runtime.InteropServices;
using Sparrow.Binary;
using Sparrow.Global;

namespace Sparrow
{
    public interface IPoolAllocatorOptions : INativeOptions
    {
        IAllocatorComposer<Pointer> CreateAllocator();
    }

    public static class PoolAllocator
    {
        [StructLayout(LayoutKind.Explicit, Size = 20)]
        internal struct PoolHeader
        {
            [FieldOffset(0)]
            public int Size;

            [FieldOffset(4)]
            public Pointer Header;            
        }

        public struct Default : IPoolAllocatorOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public IAllocatorComposer<Pointer> CreateAllocator() => new Allocator<NativeAllocator<NativeAllocator.Default>>();
        }
    }

    public unsafe struct PoolAllocator<TOptions> : IAllocator<PoolAllocator<TOptions>, Pointer>, IAllocator, IDisposable, ILowMemoryHandler<PoolAllocator<TOptions>>, IRenewable<PoolAllocator<TOptions>>
        where TOptions : struct, IPoolAllocatorOptions
    {
        private TOptions _options;

        private int _allocated;
        private int _used;

        private PoolAllocator.PoolHeader*[] _freed;
        private IAllocatorComposer<Pointer> _internalAllocator;


        public int Allocated => _allocated;
        public int Used => _used;

        public void Initialize(ref PoolAllocator<TOptions> allocator)
        {
            // Initialize the struct pointers structure used to navigate over the allocated memory.
            allocator._freed = new PoolAllocator.PoolHeader*[32];            
        }

        public void Configure<TConfig>(ref PoolAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(TOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TOptions)}");

            // This cast will get evicted by the JIT. 
            allocator._options = (TOptions)(object)configuration;
            // PERF: This should be devirtualized. 
            allocator._internalAllocator = allocator._options.CreateAllocator();
        }

        public Pointer Allocate(ref PoolAllocator<TOptions> allocator, int size)
        {
            //if (allocator.__ptrStart == null)
            //    ThrowInvalidAllocateFromResetWithoutRenew();

            size = Bits.NextPowerOf2(size);

            var index = Bits.MostSignificantBit(size) - 1;
            if (_freed[index] != null)
            {                
                var section = _freed[index];

                // Pointer was holding the next released block instead. 
                _freed[index] = (PoolAllocator.PoolHeader*) section->Header.Ptr; 

                allocator._used += section->Size;

                //return new Pointer(&section->Header);
            }

            allocator._used += size;

            //BlockPointer nativePtr = _internalAllocator.Allocate(size + sizeof(PoolBlockAllocator.PoolBlockHeader));

            //var blockPtr = (PoolBlockAllocator.PoolBlockHeader*) nativePtr._header;
            //nativePtr._header->Ptr = 
            //new BlockPointer( )


            throw new NotImplementedException();
        }

        public void Release(ref PoolAllocator<TOptions> allocator, ref Pointer ptr)
        {
            throw new NotImplementedException();
        }


        private static void ThrowInvalidAllocateFromResetWithoutRenew()
        {
            throw new InvalidOperationException("Attempt to allocate from reset arena without calling renew");
        }

        private static void ThrowAlreadyDisposedException()
        {
            throw new ObjectDisposedException("This ArenaMemoryAllocator is already disposed");
        }

        public void Renew(ref PoolAllocator<TOptions> allocator)
        {
            throw new NotImplementedException();
        }

        public void Reset(ref PoolAllocator<TOptions> allocator)
        {
            throw new NotImplementedException();
        }

        public void OnAllocate(ref PoolAllocator<TOptions> allocator, Pointer ptr)
        {
            throw new NotImplementedException();
        }

        public void OnRelease(ref PoolAllocator<TOptions> allocator, Pointer ptr)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public void NotifyLowMemory(ref PoolAllocator<TOptions> allocator)
        {
            throw new NotImplementedException();
        }

        public void NotifyLowMemoryOver(ref PoolAllocator<TOptions> allocator)
        {
            throw new NotImplementedException();
        }
    }


}
