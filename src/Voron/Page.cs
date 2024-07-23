using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Json;
using Voron.Global;
using Voron.Impl.Paging;

namespace Voron
{
    public readonly unsafe struct Page(byte* pointer)
    {
        public readonly byte* Pointer = pointer;

        public ref T GetRef<T>() where T : unmanaged
        {
            return ref Unsafe.AsRef<T>(Pointer);
        }
        public Span<byte> AsSpan(int offset, int length)
        {
            Debug.Assert(offset + length <= (IsOverflow ? OverflowSize + PageHeader.SizeOf : Constants.Storage.PageSize));
            return new Span<byte>(Pointer + offset, length);
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

        public int GetNumberOfPages()
        {
            var pageHeader = (PageHeader*)Pointer;
            int numberOfPages = 1;
            if ((pageHeader->Flags & PageFlags.Overflow) == PageFlags.Overflow)
            {
                numberOfPages = Paging.GetNumberOfOverflowPages(pageHeader->OverflowSize);
            }
            return numberOfPages;
        }

    }
}
