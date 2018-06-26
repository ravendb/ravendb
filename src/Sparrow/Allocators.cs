using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Global;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Sparrow
{

    // The header is aligned to ensure that at least we are going to be aligned if the blockAllocator is actually requesting aligned blocks.
    // and also its size is 128 bytes to ensure that allocators can create complex structures with and have enough leeway to build interesting
    // allocation strategies. The idea of the memory block is that they are typically big. 
    [StructLayout(LayoutKind.Explicit, Size = 128)]
    public unsafe struct MemoryBlockHeader
    {
        [FieldOffset(0)]
        public void* Ptr;

        [FieldOffset(8)]
        public int Size;

        [FieldOffset(16)]
        public uint Flags;

        public MemoryBlockHeader(BlockPointer ptr, BlockPointer.HeaderFlags flags = BlockPointer.HeaderFlags.Allocated)
        {
            this.Ptr = (byte*)ptr.Ptr + sizeof(MemoryBlockHeader);
            this.Size = ptr.Size - sizeof(MemoryBlockHeader);
            this.Flags = (uint)flags;
        }

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Ptr != null; }
        }

        public bool IsAllocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Flags & (uint)BlockPointer.HeaderFlags.Allocated) != 0; }
        }
    }


    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct MemoryBlock
    {
        public readonly MemoryBlockHeader* _header;
        public readonly BlockPointer _ptr;

        public int Size
        {

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Size; }
        }

        public void* Ptr
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Ptr; }
        }

        public MemoryBlock(BlockPointer ptr)
        {
            if (!ptr.IsValid)
                ThrowArgumentNullException("The pointer cannot be null or invalid.");

            this._ptr = ptr;
            
            this._header = (MemoryBlockHeader*)ptr.Ptr; // Unsafe casting
            *this._header = new MemoryBlockHeader(ptr); // Assignment 
        }

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header != null && _header->IsValid; }
        }

        private static void ThrowArgumentNullException(string msg)
        {
            throw new ArgumentNullException(msg);
        }
    }


    [StructLayout(LayoutKind.Sequential)]
    public readonly unsafe struct Pointer
    {
        public readonly void* Ptr;

        public readonly int Size;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan()
        {
            return new Span<byte>(Ptr, Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan(int length)
        {
#if VALIDATE
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            
#endif

            return new Span<byte>(Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan(int start, int length)
        {
#if VALIDATE
            if (start + length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            
#endif

            return new Span<byte>((byte*)Ptr + start, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan()
        {
            return new ReadOnlySpan<byte>(Ptr, Size);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan(int length)
        {
#if VALIDATE
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            
#endif

            return new ReadOnlySpan<byte>(Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan(int start, int length)
        {
#if VALIDATE
            if (start + length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            
#endif

            return new ReadOnlySpan<byte>((byte*)Ptr + start, length);
        }
    }

    public readonly unsafe struct BlockPointer<T> where T : struct
    {
        [MethodImpl(MethodImplOptions.NoOptimization)]
        static BlockPointer()
        {
            // If T is not a blittable type, fail on type loading unrecoverably.
            // We need to ensure the JIT wont optimize away the call to Marshal.SizeOf<T> for correctness either now or in the future. 
            Marshal.SizeOf<T>();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public BlockPointer(BlockPointer ptr)
        {
            this._ptr = ptr;
        }

        internal readonly BlockPointer _ptr;

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _ptr.IsValid; }
        }

        public void* Ptr
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return _ptr.Ptr;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> AsSpan()
        {
            return new Span<T>(_ptr.Ptr, Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> AsSpan(int length)
        {
#if VALIDATE
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            
#endif
            return new Span<T>(Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> AsSpan(int start, int length)
        {
#if VALIDATE
            if (start + length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            
#endif
            return new Span<T>((byte*)Ptr + start * Unsafe.SizeOf<T>(), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan()
        {
            return new ReadOnlySpan<byte>(_ptr.Ptr, Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<T> AsReadOnlySpan(int length)
        {
#if VALIDATE
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            
#endif
            return new ReadOnlySpan<T>(Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<T> AsReadOnlySpan(int start, int length)
        {
#if VALIDATE
            if (start + length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            
#endif
            return new ReadOnlySpan<T>((byte*)Ptr + start * Unsafe.SizeOf<T>(), length);
        }

        public int Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                EnsureIsNotBadPointerAccess();
                // We move to the actual header which is behind.
                return _ptr.Size / Unsafe.SizeOf<T>();
            }
        }

        public int SizeAsBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                EnsureIsNotBadPointerAccess();
                // We move to the actual header which is behind.
                return _ptr.Size;
            }
        }

        [Conditional("DEBUG")]
        [Conditional("VALIDATE")]
        internal void EnsureIsNotBadPointerAccess()
        {
            if (!_ptr.IsValid)
                throw new InvalidOperationException($"Trying to access the pointer but it is not valid");
        }


        [Conditional("DEBUG")]
        [Conditional("VALIDATE")]
        internal void EnsureIsNotBadPointerAccess(int i)
        {
            if ((uint)i < (uint)Size)
                throw new ArgumentOutOfRangeException($"Trying to access the pointer at location '{i}' where size is '{Size}'");
        }

        public ref T this[int i]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                EnsureIsNotBadPointerAccess(i);
                byte* ptr = (byte*)_ptr.Ptr + i * Unsafe.SizeOf<T>();
                return ref Unsafe.AsRef<T>(ptr);
            }
        }

        // User-defined conversion from Digit to double
        public static implicit operator BlockPointer(BlockPointer<T> ptr)
        {
            return new BlockPointer(ptr._ptr._header);
        }
    }

   

    public readonly unsafe struct BlockPointer
    {
        [Flags]
        public enum HeaderFlags : uint
        {
            Allocated = 0x8000,
            Naked = 0x4000,
            Mask = 0xF000,
        }

        // The header is aligned to ensure that at least we are going to be aligned if the blockAllocator is actually requesting aligned blocks.
        // and also its size is 8 bytes to ensure that only a single cache miss happens when accessing the first primitive element or any of the pointer support data. 
        [StructLayout(LayoutKind.Explicit, Size = 16)]
        public struct Header
        {
            [FieldOffset(0)]
            public void* Ptr;

            [FieldOffset(8)]
            public int Size;

            [FieldOffset(12)]
            private uint _header;

            private const uint OffsetMask = 0x0FFF;

            public Header(void* ptr, int size, uint offset = 0, HeaderFlags flags = HeaderFlags.Allocated)
            {
                if ((offset & 0xF000) != 0)
                    throw new NotSupportedException("Offsets bigger than ");

                this.Ptr = ptr;
                this.Size = size;                
                this._header = offset | (uint)flags;
            }

            public bool IsValid
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return Ptr != null; }
            }

            public bool IsAllocated
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return (_header & (uint)HeaderFlags.Allocated) != 0; }
            }

            public bool IsNaked
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return (_header & (uint)HeaderFlags.Naked) != 0; }
            }

            public uint Offset
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return _header & OffsetMask; }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal BlockPointer(Header* header)
        {
            _header = header;
        }

        internal readonly Header* _header;

        public int Size
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Size; }
        }

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header != null && _header->IsValid; }
        }

        public void* Ptr
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _header->Ptr; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan()
        {
            return new Span<byte>(_header->Ptr, Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan(int length)
        {
#if VALIDATE
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            
#endif

            return new Span<byte>(_header->Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan(int start, int length)
        {
#if VALIDATE
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            
#endif

            return new Span<byte>((byte*)_header->Ptr + start, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan()
        {
            return new ReadOnlySpan<byte>(_header->Ptr, Size);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan(int length)
        {
#if VALIDATE
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            
#endif

            return new ReadOnlySpan<byte>(_header->Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan(int start, int length)
        {
#if VALIDATE
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            
#endif

            return new ReadOnlySpan<byte>((byte*)_header->Ptr + start, length);
        }

        public ref byte this[int i]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                EnsureIsNotBadPointerAccess(i);
                return ref ((byte*)_header->Ptr)[i];
            }
        }

        [Conditional("DEBUG")]
        [Conditional("VALIDATE")]
        internal void EnsureIsNotBadPointerAccess(int i)
        {
            if ((uint)i < (uint)_header->Size)
                throw new ArgumentOutOfRangeException($"Trying to access the pointer at location '{i}' where size is '{_header->Size}'");
        }
    }

    public static class Allocators
    {
        //    public readonly AllocatorBuilder<PoolAllocator> Pool = new AllocatorBuilder<PoolAllocator>();
        //    public readonly AllocatorBuilder<ArenaAllocator> Arena = new AllocatorBuilder<ArenaAllocator>();
    }


    public class Allocator
    {
        //public static Allocator Create<T>(AllocatorBuilder<T> blockAllocator) where T : struct, IAllocator
        //{
        //    throw new NotImplementedException();
        //}
    }

    public interface IAllocatorOptions
    {
    }

    public interface ILifecycleHandler<TAllocator> where TAllocator : struct, IAllocator<TAllocator>, IAllocator, IDisposable
    {
        void BeforeInitialize(ref TAllocator allocator);
        void AfterInitialize(ref TAllocator allocator);
        void BeforeDispose(ref TAllocator allocator);
        void BeforeFinalization(ref TAllocator allocator);
    }

    public interface ILowMemoryHandler<TAllocator> where TAllocator : struct, IAllocator<TAllocator>, IAllocator, IDisposable
    {
        void NotifyLowMemory(ref TAllocator allocator);
        void NotifyLowMemoryOver(ref TAllocator allocator);
    }

    public interface IRenewable<TAllocator> where TAllocator : struct, IAllocator<TAllocator>, IAllocator, IDisposable
    {
        void Renew(ref TAllocator allocator);
    }

    public interface IAllocator { }

    public unsafe interface IAllocator<T> where T : struct, IAllocator, IDisposable
    {
        int Allocated { get; }

        void Initialize(ref T allocator);

        void Configure<TConfig>(ref T allocator, ref TConfig configuration)
            where TConfig : struct, IAllocatorOptions;

        void Allocate(ref T allocator, int size, out BlockPointer.Header* header);
        void Release(ref T allocator, in BlockPointer.Header* header);
        void Reset(ref T allocator);

        void OnAllocate(ref T allocator, BlockPointer ptr);
        void OnRelease(ref T allocator, BlockPointer ptr);
    }

    public sealed class Allocator<TAllocator> : IDisposable, ILowMemoryHandler
        where TAllocator : struct, IAllocator<TAllocator>, IAllocator, IDisposable
    {
        private TAllocator _allocator;
        private readonly SingleUseFlag _disposeFlag = new SingleUseFlag();

        ~Allocator()
        {
            if (_allocator is ILifecycleHandler<TAllocator> a)
                a.BeforeFinalization(ref _allocator);

            Dispose();
        }

        public void Initialize<TBlockAllocatorOptions>(TBlockAllocatorOptions options)
            where TBlockAllocatorOptions : struct, IAllocatorOptions
        {

            if (_allocator is ILifecycleHandler<TAllocator> a)
                a.BeforeInitialize(ref _allocator);

            _allocator.Initialize(ref _allocator);
            _allocator.Configure(ref _allocator, ref options);

            if (_allocator is ILifecycleHandler<TAllocator> b)
                b.AfterInitialize(ref _allocator);
        }

        public int Allocated
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _allocator.Allocated; }
        }
        
        public BlockPointer Allocate(int size)
        {
            unsafe
            {
                _allocator.Allocate(ref _allocator, size, out var header);

                var ptr = new BlockPointer(header);
                if (_allocator is ILifecycleHandler<TAllocator> a)
                    a.BeforeInitialize(ref _allocator);

                return ptr;
            }
        }

        public BlockPointer<TType> Allocate<TType>(int size) where TType : struct
        {
            unsafe
            {
                _allocator.Allocate(ref _allocator, size * Unsafe.SizeOf<TType>(), out var header);

                var ptr = new BlockPointer(header);

                // PERF: We cannot make this conditional because the runtime cost would kill us (too much traffic).
                //       But we can call it anyways and use the capability of evicting the call if empty.
                _allocator.OnAllocate(ref _allocator, ptr);

                return new BlockPointer<TType>(ptr);
            }
        }

        public void Release<TType>(ref BlockPointer<TType> ptr) where TType : struct
        {
            unsafe
            {
                // PERF: We cannot make this conditional because the runtime cost would kill us (too much traffic).
                //       But we can call it anyways and use the capability of evicting the call if empty.
                _allocator.OnRelease(ref _allocator, ptr);

                _allocator.Release(ref _allocator, in ptr._ptr._header);

                ptr = new BlockPointer<TType>();
            }
        }

        public void Release(ref BlockPointer ptr)
        {
            unsafe
            {
                // PERF: We cannot make this conditional because the runtime cost would kill us (too much traffic).
                //       But we can call it anyways and use the capability of evicting the call if empty.
                _allocator.OnRelease(ref _allocator, ptr);

                _allocator.Release(ref _allocator, in ptr._header);

                ptr = new BlockPointer();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Renew()
        {
            if (_allocator is IRenewable<TAllocator> a)
                a.Renew(ref _allocator);
            else
                throw new NotSupportedException($".{nameof(Renew)}() is not supported for this allocator type.");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reset()
        {
            _allocator.Reset(ref _allocator);
        }
        
        public void Dispose()
        {
            if (_disposeFlag.Raise())
                _allocator.Dispose();

            GC.SuppressFinalize(this);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void LowMemory()
        {
            if (_allocator is ILowMemoryHandler<TAllocator> a)
                a.NotifyLowMemory(ref _allocator);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void LowMemoryOver()
        {
            if (_allocator is ILowMemoryHandler<TAllocator> a)
                a.NotifyLowMemoryOver(ref _allocator);
        }
    }

    public interface INativeBlockOptions : IAllocatorOptions
    {
        bool UseSecureMemory { get; }
        bool ElectricFenceEnabled { get; }
        bool Zeroed { get; }
    }

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
        ThreadAffineWorkload Workload { get; }
    }

    public static class ThreadAffineBlockAllocator
    {
        public struct Default : IThreadAffineBlockOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;
            public int BlockSize => 4 * Constants.Size.Kilobyte;
            public ThreadAffineWorkload Workload => ThreadAffineWorkload.Default;
        }
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

    public unsafe struct ThreadAffineBlockAllocator<TOptions> : IAllocator<ThreadAffineBlockAllocator<TOptions>>, IAllocator, IDisposable, ILowMemoryHandler<ThreadAffineBlockAllocator<TOptions>>
        where TOptions : struct, IThreadAffineBlockOptions
    {
        private TOptions _options;
        private NativeBlockAllocator<TOptions> _nativeAllocator;
        private Container[] _container;

        private struct Container
        {
            public IntPtr _block1;
            public IntPtr _block2;
            public IntPtr _block3;
            public IntPtr _block4;
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
        }

        public void Allocate(ref ThreadAffineBlockAllocator<TOptions> allocator, int size, out BlockPointer.Header* header)
        {
            if (size < allocator._options.BlockSize)
            {
                // PERF: Bitwise add should emit a 'and' instruction followed by a constant.
                int threadId = Thread.CurrentThread.ManagedThreadId & ((int)allocator._options.Workload - 1);

                ref Container container = ref allocator._container[threadId];

                header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container._block1, IntPtr.Zero, container._block1);
                if (header != null)
                    return;

                header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container._block2, IntPtr.Zero, container._block2);
                if (header != null)
                    return;

                header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container._block3, IntPtr.Zero, container._block3);
                if (header != null)
                    return;

                header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container._block4, IntPtr.Zero, container._block4);
                if (header != null)
                    return;
            }

            allocator._nativeAllocator.Allocate(ref allocator._nativeAllocator, size, out header);
        }

        public void Release(ref ThreadAffineBlockAllocator<TOptions> allocator, in BlockPointer.Header* header)
        {
            if (header->Size < allocator._options.BlockSize)
            {
                // PERF: Bitwise add should emit a and instruction followed by a constant.
                int threadId = Thread.CurrentThread.ManagedThreadId & ((int)allocator._options.Workload - 1);

                ref Container container = ref allocator._container[threadId];

                if (Interlocked.CompareExchange(ref container._block1, (IntPtr)header, IntPtr.Zero) == IntPtr.Zero)
                    return;
                if (Interlocked.CompareExchange(ref container._block2, (IntPtr)header, IntPtr.Zero) == IntPtr.Zero)
                    return;
                if (Interlocked.CompareExchange(ref container._block3, (IntPtr)header, IntPtr.Zero) == IntPtr.Zero)
                    return;
                if (Interlocked.CompareExchange(ref container._block4, (IntPtr)header, IntPtr.Zero) == IntPtr.Zero)
                    return;
            }

            allocator._nativeAllocator.Release(ref allocator._nativeAllocator, in header);
        }

        public void Reset(ref ThreadAffineBlockAllocator<TOptions> allocator)
        {
            throw new NotSupportedException($"{nameof(ThreadAffineBlockAllocator<TOptions>)} does not support '.{nameof(Reset)}()'");
        }

        public void OnAllocate(ref ThreadAffineBlockAllocator<TOptions> allocator, BlockPointer ptr) {}
        public void OnRelease(ref ThreadAffineBlockAllocator<TOptions> allocator, BlockPointer ptr) {}

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

                BlockPointer.Header* header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container._block1, IntPtr.Zero, container._block1);
                if (header != null)
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, in header);

                header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container._block2, IntPtr.Zero, container._block2);
                if (header != null)
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, in header);

                header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container._block3, IntPtr.Zero, container._block3);
                if (header != null)
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, in header);

                header = (BlockPointer.Header*)Interlocked.CompareExchange(ref container._block4, IntPtr.Zero, container._block4);
                if (header != null)
                    allocator._nativeAllocator.Release(ref allocator._nativeAllocator, in header);
            }
        }

        public void NotifyLowMemoryOver(ref ThreadAffineBlockAllocator<TOptions> allocator)
        {
            // Nothing to do here. 
        }
    }

    public struct NativeBlockAllocator<TOptions> : IAllocator<NativeBlockAllocator<TOptions>>, IAllocator, IDisposable, ILowMemoryHandler<NativeBlockAllocator<TOptions>>
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
        public unsafe void Allocate(ref NativeBlockAllocator<TOptions> allocator, int size, out BlockPointer.Header* header)
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

            header = (BlockPointer.Header*)memory;
            *header = new BlockPointer.Header(memory + sizeof(BlockPointer.Header), size);

            allocator.Allocated += size + sizeof(BlockPointer.Header);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Release(ref NativeBlockAllocator<TOptions> blockAllocator, in BlockPointer.Header* header)
        {
            blockAllocator.Allocated -= header->Size + sizeof(BlockPointer.Header);            

            // PERF: Given that for the normal use case the INativeBlockOptions we will use returns constants the
            //       JIT will be able to fold all this if sequence into a branchless single call.
            if (blockAllocator._options.ElectricFenceEnabled)
                ElectricFencedMemory.Free((byte*)header);
            else if (blockAllocator._options.UseSecureMemory)
                throw new NotImplementedException();
            else
                NativeMemory.Free((byte*) header, header->Size + sizeof(BlockPointer.Header));          
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
