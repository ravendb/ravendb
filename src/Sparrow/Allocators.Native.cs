using System;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
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

    public interface INativeGlobalAllocatorOptions { }

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
    /// this allocator can leak if used improperly. This allocator is Thread-Safe.
    /// </summary>
    /// <typeparam name="TOptions">The options to use for the allocator.</typeparam>
    /// <remarks>The Options object must be properly implemented to achieve performance improvements. (use constants as much as you can)</remarks>
    public unsafe struct NativeAllocator<TOptions> : IAllocator<NativeAllocator<TOptions>, Pointer>, IAllocator
        where TOptions : struct, INativeOptions
    {
        private TOptions _options;
        private long _totalAllocated;
        private long _allocated;

        public void Configure<TConfig>(ref NativeAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(TOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} is not compatible with {nameof(TOptions)}");

            // This cast will get evicted by the JIT. 
            allocator._options = (TOptions)(object)configuration;

            if (((TOptions)(object)configuration).ElectricFenceEnabled && ((TOptions)(object)configuration).UseSecureMemory)
                throw new NotSupportedException($"{nameof(TConfig)} is asking for secure, electric fenced memory. The combination is not supported.");
        }

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
            get { return _allocated; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Initialize(ref NativeAllocator<TOptions> allocator)
        {
            allocator._totalAllocated = 0;
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

            Interlocked.Add(ref allocator._allocated, size);
            Interlocked.Add(ref allocator._totalAllocated, size);

            return new Pointer(memory, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ref NativeAllocator<TOptions> allocator, ref Pointer ptr)
        {
            Interlocked.Add(ref allocator._allocated, -ptr.Size);

            // PERF: Given that for the normal use case the INativeOptions we will use returns constants the
            //       JIT will be able to fold all this if sequence into a branchless single call.
            if (allocator._options.ElectricFenceEnabled)
                ElectricFencedMemory.Free((byte*)ptr.Ptr);
            else if (allocator._options.UseSecureMemory)
                throw new NotImplementedException();
            else
                NativeMemory.Free((byte*)ptr.Ptr, ptr.Size);

            ptr = new Pointer();
        }

        public void Reset(ref NativeAllocator<TOptions> allocator)
        {
            // There is no reset action to do on this allocator. We wont fail, but nothing we can do to 'reclaim' or 'reuse' memory.            
            CheckForLeaks(ref allocator);
        }

        public void OnAllocate(ref NativeAllocator<TOptions> allocator, Pointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void OnRelease(ref NativeAllocator<TOptions> allocator, Pointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void Dispose(ref NativeAllocator<TOptions> allocator)
        {
            // Global allocators checks are built around the idea that the memory allocated by this allocator
            // is static. It will go out of scope only when the process exit.
            if (allocator._options is INativeGlobalAllocatorOptions)
                return;

            CheckForLeaks(ref allocator);
        }

        [Conditional("DEBUG")]
        [Conditional("VALIDATE")]
        private void CheckForLeaks(ref NativeAllocator<TOptions> allocator)
        {
            if (allocator.Allocated != 0)
            {
                throw new InvalidOperationException($"The allocator is leaking '{allocator.Allocated}' bytes of memory.");
            }
        }
    }
}
