using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

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

    [StructLayout(LayoutKind.Explicit)]
    public readonly unsafe struct UnmanagedSpan
    {
        [FieldOffset(0)] 
        public readonly byte* Address;
        [FieldOffset(8)] // ensure that even on 32 bits, we have 8 bytes space
        public readonly int Length;

        [FieldOffset(0)]
        public readonly long Long;
        
        [FieldOffset(0)]
        public readonly double Double;

        public UnmanagedMemoryStream ToStream() => new(Address, Length);

        public override string ToString()
        {
            if (Length == -1)
            {
                return Long.ToString();
            }

            return $"{(long)Address:x8}, Len: {Length}";
        }

        public static UnmanagedSpan Empty = new UnmanagedSpan(null, 0);

        public UnmanagedSpan(long l)
        {
            Long = l;
            Length = -1;
        }

        public UnmanagedSpan(double d)
        {
            Double = d;
            Length = -1;
        }
        
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
            Debug.Assert(Length >= 0);
            return new UnmanagedSpan(Address + offset, Length - offset);
        }

        public UnmanagedSpan Slice(int position, int length)
        {
            Debug.Assert(Length >= 0);
            return new UnmanagedSpan(Address + position, length);
        }

        public Span<byte> ToSpan() => new Span<byte>(Address, Length);
        public ReadOnlySpan<byte> ToReadOnlySpan() => new ReadOnlySpan<byte>(Address, Length);

        public static UnmanagedSpan operator +(UnmanagedSpan pointer, int offset) => new UnmanagedSpan(pointer.Address + offset, pointer.Length - offset);

        public static implicit operator Span<byte>(UnmanagedSpan pointer) => new Span<byte>(pointer.Address, pointer.Length);
        public static implicit operator ReadOnlySpan<byte>(UnmanagedSpan pointer) => new ReadOnlySpan<byte>(pointer.Address, pointer.Length);

        public string ToStringValue() => Length == 0 ? string.Empty : Encoding.UTF8.GetString(Address, Length);
    }

    public unsafe struct UnmanagedSpanComparer : IEqualityComparer<UnmanagedSpan>
    {
        public static UnmanagedSpanComparer Instance = new();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(UnmanagedSpan x, UnmanagedSpan y)
        {
            if (x.Length != y.Length)
                return false;

            if (x.Address == y.Address)
                return true;

            return Memory.CompareInline(x.Address, y.Address, x.Length) == 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(UnmanagedSpan item)
        {
            return (int)Hashing.Marvin32.CalculateInline(item.Address, item.Length);
        }
    }
}
