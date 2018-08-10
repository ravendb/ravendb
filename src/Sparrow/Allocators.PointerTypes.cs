using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Sparrow
{
    public interface IPointerType { }
    public interface IPointerType<T> : IPointerType where T : struct { }

    [StructLayout(LayoutKind.Sequential)]
#if !VALIDATE
    // We do this to have the ability to perform extensive validation for pointers.
    readonly
#endif
    public unsafe struct Pointer : IPointerType, Meta.IDescribe
        {
    public readonly void* Ptr;

        public readonly int Size;

#if VALIDATE
        public uint Generation;        
#endif

        public Pointer(void* ptr, int size)
        {
            this.Ptr = ptr;
            this.Size = size;
#if VALIDATE
            this.Generation = 0;
#endif
        }

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Ptr != null; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan()
        {
            return new Span<byte>(Ptr, (int)Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan(int length)
        {
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

            return new Span<byte>(Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan(int start, int length)
        {
            if (start + length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

            return new Span<byte>((byte*)Ptr + start, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan()
        {
            return new ReadOnlySpan<byte>(Ptr, (int)Size);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan(int length)
        {
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

            return new ReadOnlySpan<byte>(Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan(int start, int length)
        {
            if (start + length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

            return new ReadOnlySpan<byte>((byte*)Ptr + start, length);
        }

        public static implicit operator Pointer(Pointer<byte> ptr)
        {
            return new Pointer(ptr.Ptr, ptr.SizeAsBytes);
        }

        public string Describe()
        {
            if (this.IsValid)
                return $"{{{(long)this.Ptr:X32}|{this.Size}}}";

            return "{null}";
        }

        public override string ToString()
        {
            return Describe();
        }
    }

    [StructLayout(LayoutKind.Sequential)]
#if !VALIDATE
    // We do this to have the ability to perform extensive validation for pointers.
    readonly
#endif
    public unsafe struct Pointer<T> : Meta.IDescribe, IPointerType<T> where T : struct
    {
        public readonly void* Ptr;

        private readonly int _size;

#if VALIDATE
        public uint Generation;        
#endif

        [MethodImpl(MethodImplOptions.NoOptimization)]
        static Pointer()
        {
            // If T is not a blittable type, fail on type loading unrecoverably.
            // We need to ensure the JIT wont optimize away the call to Marshal.SizeOf<T> for correctness either now or in the future. 
            Marshal.SizeOf<T>();
        }

        public Pointer(Pointer ptr)
        {
            this.Ptr = ptr.Ptr;
            this._size = ptr.Size / Unsafe.SizeOf<T>();

#if VALIDATE
            this.Generation = ptr.Generation;
#endif
        }

        public Pointer(void* ptr, int size)
        {
            this.Ptr = ptr;
            this._size = size;

#if VALIDATE
            this.Generation = 0;
#endif
        }

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Ptr != null; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> AsSpan()
        {
            return new Span<T>(Ptr, (int)Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> AsSpan(int length)
        {
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

            return new Span<T>(Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> AsSpan(int start, int length)
        {
            if (start + length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

            return new Span<T>((byte*)Ptr + start * Unsafe.SizeOf<T>(), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan()
        {
            return new ReadOnlySpan<byte>(Ptr, (int)Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<T> AsReadOnlySpan(int length)
        {
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

            return new ReadOnlySpan<T>(Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<T> AsReadOnlySpan(int start, int length)
        {
            if (start + length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

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

        public string Describe()
        {
            if (this.IsValid)
                return $"{{{(long)this.Ptr:X32}|{this.Size}|{this.SizeAsBytes}b}}";

            return "{null}";
        }

        public override string ToString()
        {
            return Describe();
        }

        [Conditional("DEBUG")]
        [Conditional("VALIDATE")]
        internal void EnsureIsNotBadPointerAccess()
        {
            if (Ptr == null)
                throw new InvalidOperationException($"Trying to access the pointer but it is not valid");
        }

        public static implicit operator Pointer(Pointer<T> ptr)
        {
            return new Pointer(ptr.Ptr, ptr.SizeAsBytes);
        }
    }


    /// <summary>
    /// The BlockPointer is a type of pointer that carries with it 2 different sizes at the same time. The pointer value
    /// with the proper memory size allocated to it AND the actual requested size when asked. A BlockPointer created from
    /// a straight Pointer will have the same size for the allocation size and the actual size. 
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
#if !VALIDATE
    // We do this to have the ability to perform extensive validation for pointers.
    readonly
#endif
    public unsafe struct BlockPointer : IPointerType, Meta.IDescribe
    {
        public readonly void* Ptr;

        public readonly int Size;

        public readonly int BlockSize;

#if VALIDATE
        public uint Generation;        
#endif

        public BlockPointer(void* ptr, int blockSize, int size)
        {
            this.Ptr = ptr;
            this.Size = size;
            this.BlockSize = blockSize;

#if VALIDATE
            this.Generation = 0;
#endif
        }

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Ptr != null; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan()
        {
            return new Span<byte>(Ptr, (int)Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan(int length)
        {
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

            return new Span<byte>(Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<byte> AsSpan(int start, int length)
        {
            if (start + length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

            return new Span<byte>((byte*)Ptr + start, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan()
        {
            return new ReadOnlySpan<byte>(Ptr, (int)Size);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan(int length)
        {
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

            return new ReadOnlySpan<byte>(Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan(int start, int length)
        {
            if (start + length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

            return new ReadOnlySpan<byte>((byte*)Ptr + start, length);
        }

        public string Describe()
        {
            if (this.IsValid)
                return $"{{{(long)this.Ptr:X32}|{this.Size}|{this.BlockSize}}}";

            return "{null}";
        }

        public override string ToString()
        {
            return Describe();
        }

        public static implicit operator BlockPointer(Pointer ptr)
        {
            return new BlockPointer(ptr.Ptr, ptr.Size, ptr.Size);
        }
    }

    /// <summary>
    /// The BlockPointer is a type of pointer that carries with it 2 different sizes at the same time. The pointer value
    /// with the proper memory size allocated to it AND the actual requested size when asked. A BlockPointer created from
    /// a straight Pointer will have the same size for the allocation size and the actual size. 
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
#if !VALIDATE
    // We do this to have the ability to perform extensive validation for pointers.
    readonly
#endif
    public unsafe struct BlockPointer<T> : Meta.IDescribe, IPointerType<T> where T : struct
    {
        public readonly void* Ptr;

        private readonly int _size;

        public readonly int BlockSize;

#if VALIDATE
        public uint Generation;        
#endif

        [MethodImpl(MethodImplOptions.NoOptimization)]
        static BlockPointer()
        {
            // If T is not a blittable type, fail on type loading unrecoverably.
            // We need to ensure the JIT wont optimize away the call to Marshal.SizeOf<T> for correctness either now or in the future. 
            Marshal.SizeOf<T>();
        }

        public BlockPointer(BlockPointer ptr)
        {
            this.Ptr = ptr.Ptr;
            this._size = ptr.Size / Unsafe.SizeOf<T>();
            this.BlockSize = ptr.BlockSize;

#if VALIDATE
            this.Generation = ptr.Generation;
#endif
        }

        public BlockPointer(void* ptr, int blockSize, int size)
        {
            this.Ptr = ptr;
            this._size = size;
            this.BlockSize = blockSize;

#if VALIDATE
            this.Generation = 0;
#endif
        }

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Ptr != null; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> AsSpan()
        {
            return new Span<T>(Ptr, (int)Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> AsSpan(int length)
        {
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

            return new Span<T>(Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Span<T> AsSpan(int start, int length)
        {
            if (start + length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            
            
            return new Span<T>((byte*)Ptr + start * Unsafe.SizeOf<T>(), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<byte> AsReadOnlySpan()
        {
            return new ReadOnlySpan<byte>(Ptr, (int)Size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<T> AsReadOnlySpan(int length)
        {
            if (length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

            return new ReadOnlySpan<T>(Ptr, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<T> AsReadOnlySpan(int start, int length)
        {
            if (start + length > Size)
                throw new ArgumentException($"{nameof(length)} cannot be bigger than block size.");            

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

        public string Describe()
        {
            if (this.IsValid)
                return $"{{{(long)this.Ptr:X64}|{this.Size}|{this.BlockSize}|{this.SizeAsBytes}b}}";

            return "{null}";
        }

        public override string ToString()
        {
            return Describe();
        }

        public static implicit operator BlockPointer(BlockPointer<T> ptr)
        {
            return new BlockPointer(ptr.Ptr, ptr.BlockSize, ptr.Size);
        }

        public static implicit operator BlockPointer<T>(Pointer<T> ptr)
        {
            return new BlockPointer<T>(ptr.Ptr, ptr.SizeAsBytes, ptr.SizeAsBytes);
        }

        public static implicit operator BlockPointer<T>(BlockPointer ptr)
        {
            return new BlockPointer<T>(ptr.Ptr, ptr.BlockSize, ptr.Size);
        }
    }
}
