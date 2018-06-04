using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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

        private readonly BlockPointer _ptr;

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
                EnsureIsNotBadPointerAccess();
                return _ptr.Ptr;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> AsSpan()
        {
            EnsureIsNotBadPointerAccess();
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
            EnsureIsNotBadPointerAccess();
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
                // We move to the actual header which is behind.
                return _ptr.Size / Unsafe.SizeOf<T>();
            }
        }

        [Conditional("DEBUG")]
        [Conditional("VALIDATE")]
        internal void EnsureIsNotBadPointerAccess()
        {
            if (!_ptr.IsValid)
                throw new ArgumentOutOfRangeException($"Trying to access the pointer but it is not valid");
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
            get { return _header->IsValid; }
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

    public interface IAllocationHandler<TAllocator> where TAllocator : struct, IAllocator<TAllocator>, IAllocator, IDisposable
    {
        void OnAllocate(ref TAllocator allocator, BlockPointer ptr);
        void OnRelease(ref TAllocator allocator, BlockPointer ptr);
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
    }

    public sealed class Allocator<TAllocator> : IDisposable, ILowMemoryHandler
        where TAllocator : struct, IAllocator<TAllocator>, IAllocator, IDisposable
    {
        private TAllocator _allocator;
        private readonly SingleUseFlag _disposeFlag = new SingleUseFlag();

        ~Allocator()
        {
            if (_allocator.GetType() == typeof(ILifecycleHandler<TAllocator>))
                ((ILifecycleHandler<TAllocator>)_allocator).BeforeFinalization(ref _allocator);

            Dispose();
        }

        public void Initialize<TBlockAllocatorOptions>(TBlockAllocatorOptions options)
            where TBlockAllocatorOptions : struct, IAllocatorOptions
        {
            if (_allocator.GetType() == typeof(ILifecycleHandler<TAllocator>))
                ((ILifecycleHandler<TAllocator>)_allocator).BeforeInitialize(ref _allocator);

            _allocator.Initialize(ref _allocator);
            _allocator.Configure(ref _allocator, ref options);

            if (_allocator.GetType() == typeof(ILifecycleHandler<TAllocator>))
                ((ILifecycleHandler<TAllocator>)_allocator).AfterInitialize(ref _allocator);
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
                if (_allocator.GetType() == typeof(IAllocationHandler<TAllocator>))
                    ((IAllocationHandler<TAllocator>)_allocator).OnAllocate(ref _allocator, ptr);

                return ptr;
            }
        }

        public BlockPointer<TType> Allocate<TType>(int size) where TType : struct
        {
            unsafe
            {
                _allocator.Allocate(ref _allocator, size * Unsafe.SizeOf<TType>(), out var header);

                var ptr = new BlockPointer(header);
                if (_allocator.GetType() == typeof(IAllocationHandler<TAllocator>))
                    ((IAllocationHandler<TAllocator>)_allocator).OnAllocate(ref _allocator, ptr);

                return new BlockPointer<TType>(ptr);
            }
        }

        public void Release(BlockPointer ptr)
        {
            unsafe
            {
                if (_allocator.GetType() == typeof(IAllocationHandler<TAllocator>))
                    ((IAllocationHandler<TAllocator>)_allocator).OnRelease(ref _allocator, ptr);

                _allocator.Release(ref _allocator, in ptr._header);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Renew()
        {
            if (_allocator.GetType() == typeof(IRenewable<TAllocator>))
                ((IRenewable<TAllocator>)_allocator).Renew(ref _allocator);
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
            if (_allocator.GetType() == typeof(ILowMemoryHandler<TAllocator>))
                ((ILowMemoryHandler<TAllocator>)_allocator).NotifyLowMemory(ref _allocator);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void LowMemoryOver()
        {
            if (_allocator.GetType() == typeof(ILowMemoryHandler<TAllocator>))
                ((ILowMemoryHandler<TAllocator>)_allocator).NotifyLowMemoryOver(ref _allocator);
        }
    }

    public interface INativeBlockOptions : IAllocatorOptions
    {
        bool UseSecureMemory { get; }
        bool ElectricFenceEnabled { get; }
    }

    public static class NativeBlockAllocator
    {
        public struct DefaultOptions : INativeBlockOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => false;
        }

        public struct SecureOptions : INativeBlockOptions
        {
            public bool UseSecureMemory => true;
            public bool ElectricFenceEnabled => false;
        }

        public struct ElectricFenceOptions : INativeBlockOptions
        {
            public bool UseSecureMemory => false;
            public bool ElectricFenceEnabled => true;
        }
    }

    public struct NativeBlockAllocator<TOptions> : IAllocator<NativeBlockAllocator<TOptions>>, IAllocator, IDisposable, ILowMemoryHandler<NativeBlockAllocator<TOptions>>
        where TOptions : struct, INativeBlockOptions
    {
        private TOptions Options;

        public void Configure<TConfig>(ref NativeBlockAllocator<TOptions> blockAllocator, ref TConfig configuration) where TConfig : struct, IAllocatorOptions
        {
            if (!typeof(INativeBlockOptions).GetTypeInfo().IsAssignableFrom(typeof(TConfig)))
                throw new NotSupportedException($"{nameof(TConfig)} does not implements {nameof(INativeBlockOptions)}");

            // This cast will get evicted by the JIT. 
            this.Options = (TOptions)(object)configuration;

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
        public void Initialize(ref NativeBlockAllocator<TOptions> blockAllocator)
        {
            blockAllocator.Allocated = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Allocate(ref NativeBlockAllocator<TOptions> blockAllocator, int size, out BlockPointer.Header* header)
        {
            byte* memory;
            int allocatedSize = size + sizeof(BlockPointer.Header);

            // PERF: Given that for the normal use case the INativeBlockOptions we will use returns constants the
            //       JIT will be able to fold all this if sequence into a branchless single call.
            if (blockAllocator.Options.ElectricFenceEnabled)
                memory = ElectricFencedMemory.Allocate(allocatedSize);
            else if (blockAllocator.Options.UseSecureMemory)
                throw new NotImplementedException();
            else
                memory = NativeMemory.AllocateMemory(allocatedSize);

            header = (BlockPointer.Header*)memory;
            *header = new BlockPointer.Header(memory + sizeof(BlockPointer.Header), size);

            blockAllocator.Allocated += size + sizeof(BlockPointer.Header);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void Release(ref NativeBlockAllocator<TOptions> blockAllocator, in BlockPointer.Header* header)
        {
            blockAllocator.Allocated -= header->Size + sizeof(BlockPointer.Header);

            // PERF: Given that for the normal use case the INativeBlockOptions we will use returns constants the
            //       JIT will be able to fold all this if sequence into a branchless single call.
            if (blockAllocator.Options.ElectricFenceEnabled)
                ElectricFencedMemory.Free((byte*)header);
            else if (blockAllocator.Options.UseSecureMemory)
                throw new NotImplementedException();
            else
                NativeMemory.Free((byte*) header, header->Size);
        }

        public void Reset(ref NativeBlockAllocator<TOptions> blockAllocator)
        {
            throw new NotSupportedException($"{nameof(NativeBlockAllocator<TOptions>)} does not support '.{nameof(Reset)}()'");
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
