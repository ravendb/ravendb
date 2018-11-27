using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Global;

namespace Voron.Data.Fixed
{
    public unsafe class FixedSizeTreePage
    {
        private readonly byte* _ptr;
        private readonly int _entrySize;
        private readonly int _pageSize;

        public int LastMatch;
        public int LastSearchPosition;
        public bool Dirty;

        public FixedSizeTreePage(byte* b, int entrySize, int pageSize)
        {
            _ptr = b;
            _pageSize = pageSize;

            if (IsBranch)
                _entrySize = FixedSizeTree.BranchEntrySize;
            else
                _entrySize = entrySize;
        }

        private FixedSizeTreePageHeader* Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (FixedSizeTreePageHeader*)_ptr; }
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
            get { return _pageSize - Constants.FixedSizeTree.PageHeaderSize; }
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetKey(long key, int position)
        {
            GetEntry(position)->Key = key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetKey(int position)
        {
            return GetEntry(Pointer + StartPosition, position, _entrySize)->Key;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal FixedSizeTreeEntry* GetEntry(int position)
        {
            Debug.Assert(position >= 0 && ((position == 0 && NumberOfEntries == 0) || position < NumberOfEntries) ,$"FixedSizeTreePage: Requested an out of range entry {position} from [0-{NumberOfEntries-1}]");
            return GetEntry(Pointer + StartPosition, position, _entrySize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static FixedSizeTreeEntry* GetEntry(byte* p, int position, int size)
        {
            return (FixedSizeTreeEntry*)(p + position * size);
        }

        public void ResetStartPosition()
        {
            if (StartPosition == Constants.FixedSizeTree.PageHeaderSize)
                return;

            // we need to move it back, then add the new item
            Memory.Move(Pointer + Constants.FixedSizeTree.PageHeaderSize,
                Pointer + StartPosition,
                NumberOfEntries * (IsLeaf ? _entrySize : FixedSizeTree.BranchEntrySize));

            StartPosition = Constants.FixedSizeTree.PageHeaderSize;
        }

        public void RemoveEntry(int pos)
        {
            System.Diagnostics.Debug.Assert(pos >= 0 && pos < NumberOfEntries);
            NumberOfEntries--;

            var size = (ushort)_entrySize;
            if (pos == 0)
            {
                // optimized, just move the start position
                StartPosition += size;
                return;
            }
            // have to move the memory
            Memory.Move(Pointer + StartPosition + (pos * size),
                   Pointer + StartPosition + ((pos + 1) * size),
                   (NumberOfEntries - pos) * size);
        }
    }
}
