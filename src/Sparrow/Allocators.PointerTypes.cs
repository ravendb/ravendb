using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Sparrow
{
    public interface IPointerType { }
    public interface IPointerType<T> : IPointerType where T : struct { }

    [StructLayout(LayoutKind.Sequential)]
    public readonly unsafe struct Pointer : IPointerType
    {
        public readonly void* Ptr;

        public readonly int Size;

        public Pointer(void* ptr, int size)
        {
            this.Ptr = ptr;
            this.Size = size;
        }

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


    public readonly unsafe struct BlockPointer : IPointerType
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


    [StructLayout(LayoutKind.Sequential)]
    public readonly unsafe struct Pointer<T> : IPointerType<T> where T : struct
    {
        public readonly void* Ptr;

        public readonly int _size;

        [MethodImpl(MethodImplOptions.NoOptimization)]
        static Pointer()
        {
            // If T is not a blittable type, fail on type loading unrecoverably.
            // We need to ensure the JIT wont optimize away the call to Marshal.SizeOf<T> for correctness either now or in the future. 
            Marshal.SizeOf<T>();
        }

        public Pointer(void* ptr, int size)
        {
            this.Ptr = ptr;
            this._size = size;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> AsSpan()
        {
            return new Span<T>(Ptr, Size);
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
            return new ReadOnlySpan<byte>(Ptr, Size);
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
                return _size;
            }
        }

        public int SizeAsBytes
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                EnsureIsNotBadPointerAccess();
                // We move to the actual header which is behind.
                return _size * Unsafe.SizeOf<T>();
            }
        }

        [Conditional("DEBUG")]
        [Conditional("VALIDATE")]
        internal void EnsureIsNotBadPointerAccess()
        {
            if (Ptr == null)
                throw new InvalidOperationException($"Trying to access the pointer but it is not valid");
        }

        // User-defined conversion from Digit to double
        public static implicit operator Pointer(Pointer<T> ptr)
        {
            return new Pointer(ptr.Ptr, ptr.SizeAsBytes);
        }
    }

    public readonly unsafe struct BlockPointer<T> : IPointerType<T> where T : struct
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
}
