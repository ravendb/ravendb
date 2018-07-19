using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using Sparrow.Platform;
using Sparrow.Utils;

namespace Sparrow
{
    public interface INativeOptions : IAllocatorOptions
    {
        bool UseSecureMemory { get; }
        bool ElectricFenceEnabled { get; }
        bool Zeroed { get; }
    }

    public static class NativeAllocator
    {
        public struct Default : INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;
        }

        public struct DefaultZero : INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => true;
        }

        public struct Secure : INativeOptions
        {
            public bool UseSecureMemory => true;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;
        }

        public struct ElectricFence : INativeOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => true;
            public bool Zeroed => false;
        }
    }

    /// <summary>
    /// The NativeAllocator is the barebones allocator, it will redirect the request straight to the OS system calls.
    /// It will not keep track of allocations (except when running in validation mode), that means that
    /// this allocator can leak if used improperly. 
    /// </summary>
    /// <typeparam name="TOptions">The options to use for the allocator.</typeparam>
    /// <remarks>The Options object must be properly implemented to achieve performance improvements. (use constants as much as you can)</remarks>
    public unsafe struct NativeAllocator<TOptions> : IAllocator<NativeAllocator<TOptions>, Pointer>, IAllocator
        where TOptions : struct, INativeOptions
    {
        private TOptions _options;

        public void Configure<TConfig>(ref NativeAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(TOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TOptions)}");

            // This cast will get evicted by the JIT. 
            allocator._options = (TOptions)(object)configuration;

            if (((TOptions)(object)configuration).ElectricFenceEnabled && ((TOptions)(object)configuration).UseSecureMemory)
                throw new NotSupportedException($"{nameof(TConfig)} is asking for secure, electric fenced memory. The combination is not supported.");
        }

        public long Allocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get;
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private set;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Initialize(ref NativeAllocator<TOptions> allocator)
        {
            allocator.Allocated = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Pointer Allocate(ref NativeAllocator<TOptions> allocator, int size)
        {
            byte* memory;

            // PERF: Given that for the normal use case the INativeOptions we will use returns constants the
            //       JIT will be able to fold all this if sequence into a branchless single call.
            if (allocator._options.ElectricFenceEnabled)
                memory = ElectricFencedMemory.Allocate(size);
            else if (allocator._options.UseSecureMemory)
                throw new NotImplementedException();
            else
                memory = NativeMemory.AllocateMemory(size);

            if (allocator._options.Zeroed)
                Memory.Set(memory, 0, size);

            allocator.Allocated += size;

            return new Pointer(memory, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ref NativeAllocator<TOptions> allocator, ref Pointer ptr)
        {            
            allocator.Allocated -= ptr.Size;

            // PERF: Given that for the normal use case the INativeOptions we will use returns constants the
            //       JIT will be able to fold all this if sequence into a branchless single call.
            if (allocator._options.ElectricFenceEnabled)
                ElectricFencedMemory.Free((byte*)ptr.Ptr);
            else if (allocator._options.UseSecureMemory)
                throw new NotImplementedException();
            else
                NativeMemory.Free((byte*)ptr.Ptr, ptr.Size);
        }

        public void Reset(ref NativeAllocator<TOptions> allocator)
        {
            throw new NotSupportedException($"{nameof(NativeAllocator<TOptions>)} does not support '.{nameof(Reset)}()'");
        }

        public void OnAllocate(ref NativeAllocator<TOptions> allocator, Pointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void OnRelease(ref NativeAllocator<TOptions> allocator, Pointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void Dispose(ref NativeAllocator<TOptions> allocator) {}
    }
}
