using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Global;
using Sparrow.Json;
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

    public interface IAllocator
    {
    }

    public interface IAllocatorComposer
    {
        void Initialize<TBlockAllocatorOptions>(TBlockAllocatorOptions options) where TBlockAllocatorOptions : struct, IAllocatorOptions;

        BlockPointer Allocate(int size);
        BlockPointer<TType> Allocate<TType>(int size) where TType : struct;

        void Release(ref BlockPointer ptr);
        void Release<TType>(ref BlockPointer<TType> ptr) where TType : struct;
    }

    public unsafe interface IAllocator<T> where T : struct, IAllocator, IDisposable
    {
        int Allocated { get; }

        void Initialize(ref T allocator);

        void Configure<TConfig>(ref T allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions;

        void Allocate(ref T allocator, int size, out BlockPointer.Header* header);
        void Release(ref T allocator, in BlockPointer.Header* header);
        void Reset(ref T allocator);

        void OnAllocate(ref T allocator, BlockPointer ptr);
        void OnRelease(ref T allocator, BlockPointer ptr);
    }

    public sealed class Allocator<TAllocator> : IAllocatorComposer, IDisposable, ILowMemoryHandler
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

    public interface IArenaAllocatorOptions : INativeBlockOptions
    {
        int InitialBlockSize { get; }
        int MaxBlockSize { get; }
        IAllocatorComposer CreateAllocator();
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
            public IAllocatorComposer CreateAllocator() => new Allocator<NativeBlockAllocator<NativeBlockAllocator.Default>>();
        }

        public struct ThreadAffineDefault : IArenaAllocatorOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int InitialBlockSize => 1 * Constants.Size.Megabyte;
            public int MaxBlockSize => 16 * Constants.Size.Megabyte;
            public IAllocatorComposer CreateAllocator() => new Allocator<ThreadAffineBlockAllocator<ThreadAffineBlockAllocator.Default>>();
        }

        public struct ThreadAffineDefault<T> : IArenaAllocatorOptions where T : struct, IThreadAffineBlockOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
            public bool Zeroed => false;

            public int InitialBlockSize => 1 * Constants.Size.Megabyte;
            public int MaxBlockSize => 16 * Constants.Size.Megabyte;
            public IAllocatorComposer CreateAllocator() => new Allocator<ThreadAffineBlockAllocator<T>>();
        }
    }

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

    public unsafe struct PoolAllocator<TOptions> : IAllocator<PoolAllocator<TOptions>>, IAllocator, IDisposable, ILowMemoryHandler<PoolAllocator<TOptions>>, IRenewable<PoolAllocator<TOptions>>
        where TOptions : struct, IPoolAllocatorOptions
    {
        private TOptions _options;
        
        private int _allocated;
        private int _used;

        private PoolAllocator.FreeSection*[] _freed;


        public int Allocated => _allocated;
        public int Used => _used;

        public void Initialize(ref PoolAllocator<TOptions> allocator)
        {
            // Initialize the struct pointers structure used to navigate over the allocated memory.
            allocator._freed = new PoolAllocator.FreeSection*[32];
        }

        public void Configure<TConfig>(ref PoolAllocator<TOptions> allocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            throw new NotImplementedException();
        }

        public void Allocate(ref PoolAllocator<TOptions> allocator, int size, out BlockPointer.Header* header)
        {
            throw new NotImplementedException();
        }

        public void Release(ref PoolAllocator<TOptions> allocator, in BlockPointer.Header* header)
        {
            throw new NotImplementedException();
        }

        public void Renew(ref PoolAllocator<TOptions> allocator)
        {
            throw new NotImplementedException();
        }

        public void Reset(ref PoolAllocator<TOptions> allocator)
        {
            throw new NotImplementedException();
        }

        public void OnAllocate(ref PoolAllocator<TOptions> allocator, BlockPointer ptr)
        {
            throw new NotImplementedException();
        }

        public void OnRelease(ref PoolAllocator<TOptions> allocator, BlockPointer ptr)
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

    public unsafe struct ArenaAllocator<TOptions> : IAllocator<ArenaAllocator<TOptions>>, IAllocator, IDisposable, ILowMemoryHandler<ArenaAllocator<TOptions>>
        where TOptions : struct, IArenaAllocatorOptions
    {
        private TOptions _options;
        private IAllocatorComposer _internalAllocator;

        private byte* _ptrStart;
        private byte* _ptrCurrent;

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
        public void Allocate(ref ArenaAllocator<TOptions> allocator, int size, out BlockPointer.Header* header)
        {
            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release(ref ArenaAllocator<TOptions> blockAllocator, in BlockPointer.Header* header)
        {
        }

        public void Reset(ref ArenaAllocator<TOptions> blockAllocator)
        {
        }

        public void OnAllocate(ref ArenaAllocator<TOptions> allocator, BlockPointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void OnRelease(ref ArenaAllocator<TOptions> allocator, BlockPointer ptr)
        {
            // This allocator does not keep track of anything.
        }

        public void NotifyLowMemory(ref ArenaAllocator<TOptions> blockAllocator)
        {
            // This allocator cannot do anything with this signal.
        }

        public void NotifyLowMemoryOver(ref ArenaAllocator<TOptions> blockAllocator)
        {
            // This allocator cannot do anything with this signal.
        }

        public void Dispose()
        {
        }
    }

}
