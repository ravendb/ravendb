using System;
using System.Runtime.CompilerServices;

namespace Sparrow.Server
{
    public readonly unsafe struct UnmanagedSpan<T> where T : unmanaged
    {
        public readonly T* Address;
        public readonly int Length;

        public UnmanagedSpan(void* address, int length)
        {
            Address = (T*)address;
            Length = length / sizeof(T);
        }

        public UnmanagedSpan(UnmanagedSpan<T> pointer, int length)
        {
            Address = pointer.Address;
            Length = length;
        }

        public UnmanagedSpan(UnmanagedPointer pointer, int length)
        {
            Address = (T*)pointer.Address;
            Length = length / sizeof(T);
        }

        public UnmanagedSpan<T> Slice(int offset)
        {
            return new UnmanagedSpan<T>(Address + offset, Length - offset);
        }

        public UnmanagedSpan<T> Slice(int position, int length)
        {
            return new UnmanagedSpan<T>(Address + position, length);
        }

        public ref T this[int idx]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return ref Address[idx]; }
        }

        public Span<T> ToSpan() => new(Address, Length);
        public ReadOnlySpan<T> ToReadOnlySpan() => new(Address, Length);

        public static UnmanagedSpan<T> operator +(UnmanagedSpan<T> pointer, int offset) => new(pointer.Address + offset, pointer.Length - offset);

        public static implicit operator Span<T>(UnmanagedSpan<T> pointer) => new(pointer.Address, pointer.Length);
        public static implicit operator ReadOnlySpan<T>(UnmanagedSpan<T> pointer) => new(pointer.Address, pointer.Length);
    }
}
