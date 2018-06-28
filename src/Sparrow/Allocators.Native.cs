using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using Sparrow.Platform;
using Sparrow.Utils;

namespace Sparrow
{
    public interface INativeBlockOptions : IAllocatorOptions
    {
        bool UseSecureMemory { get; }
        bool ElectricFenceEnabled { get; }
        bool Zeroed { get; }
    }

    public static class NativeBlockAllocator
    {
        public struct Default : INativeBlockOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;
        }

        public struct DefaultZero : INativeBlockOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => true;
        }

        public struct Secure : INativeBlockOptions
        {
            public bool UseSecureMemory => true;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;
        }

        public struct ElectricFence : INativeBlockOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => true;
            public bool Zeroed => false;
        }
    }

    public unsafe struct NativeBlockAllocator<TOptions> : IAllocator<NativeBlockAllocator<TOptions>, BlockPointer>, IAllocator, IDisposable, ILowMemoryHandler<NativeBlockAllocator<TOptions>>
        where TOptions : struct, INativeBlockOptions
    {
        private TOptions _options;

        public void Configure<TConfig>(ref NativeBlockAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(TOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TOptions)}");

            // This cast will get evicted by the JIT. 
            allocator._options = (TOptions)(object)configuration;

            if (((TOptions)(object)configuration).ElectricFenceEnabled && ((TOptions)(object)configuration).UseSecureMemory)
                throw new NotSupportedException($"{nameof(TConfig)} is asking for secure, electric fenced memory. The combination is not supported.");
        }

        public int Allocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Initialize(ref NativeBlockAllocator<TOptions> allocator)
        {
            allocator.Allocated = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BlockPointer Allocate(ref NativeBlockAllocator<TOptions> allocator, int size)
        {
            byte* memory;
            int allocatedSize = size + sizeof(BlockPointer.Header);

            // PERF: Given that for the normal use case the INativeBlockOptions we will use returns constants the
            //       JIT will be able to fold all this if sequence into a branchless single call.
            if (allocator._options.ElectricFenceEnabled)
                memory = ElectricFencedMemory.Allocate(allocatedSize);
            else if (allocator._options.UseSecureMemory)
                throw new NotImplementedException();
            else
                memory = NativeMemory.AllocateMemory(allocatedSize);

            if (allocator._options.Zeroed)
                Memory.Set(memory, 0, allocatedSize);

            BlockPointer.Header* header = (BlockPointer.Header*)memory;
            *header = new BlockPointer.Header(memory + sizeof(BlockPointer.Header), size);

            allocator.Allocated += allocatedSize;

            return new BlockPointer(header);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ref NativeBlockAllocator<TOptions> allocator, ref BlockPointer ptr)
        {
            BlockPointer.Header* header = ptr._header;
            allocator.Allocated -= header->Size + sizeof(BlockPointer.Header);

            // PERF: Given that for the normal use case the INativeBlockOptions we will use returns constants the
            //       JIT will be able to fold all this if sequence into a branchless single call.
            if (allocator._options.ElectricFenceEnabled)
                ElectricFencedMemory.Free((byte*)header);
            else if (allocator._options.UseSecureMemory)
                throw new NotImplementedException();
            else
                NativeMemory.Free((byte*)header, header->Size + sizeof(BlockPointer.Header));
        }

        public void Reset(ref NativeBlockAllocator<TOptions> blockAllocator)
        {
            throw new NotSupportedException($"{nameof(NativeBlockAllocator<TOptions>)} does not support '.{nameof(Reset)}()'");
        }

        public void OnAllocate(ref NativeBlockAllocator<TOptions> allocator, BlockPointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void OnRelease(ref NativeBlockAllocator<TOptions> allocator, BlockPointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void NotifyLowMemory(ref NativeBlockAllocator<TOptions> blockAllocator)
        {
            // This allocator cannot do anything with this signal.
        }

        public void NotifyLowMemoryOver(ref NativeBlockAllocator<TOptions> blockAllocator)
        {
            // This allocator cannot do anything with this signal.
        }

        public void Dispose()
        {
        }
    }
}
