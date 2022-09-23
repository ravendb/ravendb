using System;
using System.Runtime.CompilerServices;

namespace Sparrow
{
    public readonly unsafe struct UnmanagedPointer
    {
        public readonly byte* Address;

        public UnmanagedPointer(byte* address)
        {
            Address = address;
        }

        public static UnmanagedPointer operator +(UnmanagedPointer pointer, int offset) => new UnmanagedPointer(pointer.Address + offset);
    }

    public readonly unsafe struct UnmanagedSpan
    {
        public readonly byte* Address;
        public readonly int Length;

        public UnmanagedSpan(byte* address, int length)
        {
            Address = address;
            Length = length;
        }

        public UnmanagedSpan(UnmanagedPointer pointer, int length)
        {
            Address = pointer.Address;
            Length = length;
        }

        public UnmanagedSpan Slice(int offset)
        {
            return new UnmanagedSpan(Address + offset, Length - offset);
        }

        public UnmanagedSpan Slice(int position, int length)
        {
            return new UnmanagedSpan(Address + position, length);
        }

        public Span<byte> ToSpan() => new Span<byte>(Address, Length);
        public ReadOnlySpan<byte> ToReadOnlySpan() => new ReadOnlySpan<byte>(Address, Length);

        public static UnmanagedSpan operator +(UnmanagedSpan pointer, int offset) => new UnmanagedSpan(pointer.Address + offset, pointer.Length - offset);

        public static implicit operator Span<byte>(UnmanagedSpan pointer) => new Span<byte>(pointer.Address, pointer.Length);
        public static implicit operator ReadOnlySpan<byte>(UnmanagedSpan pointer) => new ReadOnlySpan<byte>(pointer.Address, pointer.Length);
    }
}
