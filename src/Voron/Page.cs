using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Voron.Global;

namespace Voron
{
    public readonly unsafe struct Page
    {
        public readonly byte* Pointer;

        public readonly Span<byte> AsSpan() => new Span<byte>(Pointer, IsOverflow ? OverflowSize + PageHeader.SizeOf : Constants.Storage.PageSize);

        public readonly Span<byte> AsSpan(int offset, int length)
        {
            Debug.Assert(offset + length <= (IsOverflow ? OverflowSize + PageHeader.SizeOf : Constants.Storage.PageSize));
            return new Span<byte>(Pointer + offset, length);
        }

        public readonly Span<T> AsSpan<T>(int offset, int length) where T : struct
        {
            Debug.Assert(length < (IsOverflow ? OverflowSize + PageHeader.SizeOf : Constants.Storage.PageSize) - offset);
            return new Span<T>(Pointer + offset, length);
        }

        public Page(byte* pointer)
        {
            Pointer = pointer;
        }

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Pointer != null; }
        }

        public byte* DataPointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Pointer + PageHeader.SizeOf; }
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return ((PageHeader*)Pointer)->PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { ((PageHeader*)Pointer)->PageNumber = value; }
        }

        public bool IsOverflow
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (((PageHeader*)Pointer)->Flags & PageFlags.Overflow) == PageFlags.Overflow; }
        }

        public int OverflowSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return ((PageHeader*)Pointer)->OverflowSize; }
            set { ((PageHeader*)Pointer)->OverflowSize = value; }
        }

        public PageFlags Flags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return ((PageHeader*)Pointer)->Flags; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { ((PageHeader*)Pointer)->Flags = value; }
        }

        public void CopyTo(in Page dest)
        {
            Unsafe.CopyBlock(dest.Pointer, Pointer, (uint)(IsOverflow ? OverflowSize : Constants.Storage.PageSize));
        }
    }
}
