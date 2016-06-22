using System.Runtime.CompilerServices;
using Voron.Global;

namespace Voron.Data.Fixed
{
    public unsafe class FixedSizeTreePage
    {
        private readonly byte* _ptr;
        private readonly int _pageSize;
        private readonly string _source;

        public int LastMatch;
        public int LastSearchPosition;
        public bool Dirty;

        public FixedSizeTreePage(byte* b, string source, int pageSize)
        {
            _ptr = b;
            _source = source;
            _pageSize = pageSize;
        }

        private FixedSizeTreePageHeader* Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (FixedSizeTreePageHeader*)_ptr; }
        }

        public string Source
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _source; }
        }

        public long PageNumber
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->PageNumber; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->PageNumber = value; }
        }

        public FixedSizeTreePageFlags FixedTreeFlags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->TreeFlags; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->TreeFlags = value; }
        }

        public int PageSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _pageSize; }
        }

        public bool IsLeaf
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Header->TreeFlags & FixedSizeTreePageFlags.Leaf) == FixedSizeTreePageFlags.Leaf; }
        }

        public bool IsBranch
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Header->TreeFlags & FixedSizeTreePageFlags.Branch) == FixedSizeTreePageFlags.Branch; }
        }

        public bool IsOverflow
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Header->Flags & PageFlags.Overflow) == PageFlags.Overflow; }
        }

        public int PageMaxSpace
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _pageSize - Constants.FixedSizeTreePageHeaderSize; }
        }


        public ushort NumberOfEntries
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->NumberOfEntries; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->NumberOfEntries = value; }
        }

        public ushort StartPosition
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->StartPosition; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->StartPosition = value; }
        }

        public ushort ValueSize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->ValueSize; }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->ValueSize = value; }
        }

        public byte* Pointer
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _ptr; }
        }

        public override string ToString()
        {
            return "#" + PageNumber + " (count: " + NumberOfEntries + ") " + FixedTreeFlags;
        }

        public PageFlags Flags
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->Flags; }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            set { Header->Flags = value; }
        }
    }
}