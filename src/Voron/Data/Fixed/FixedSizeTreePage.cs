using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Sparrow;
using Voron.Global;

namespace Voron.Data.Fixed
{
    public sealed unsafe class FixedSizeTreePage<TVal>
        where TVal : unmanaged, IBinaryNumber<TVal>, IMinMaxValue<TVal>
    {
        private readonly byte* _ptr;
        private int _entrySize;
        private readonly int _pageSize;

        private int _tombstoneBitmapSize;

        public int LastMatch;
        public int LastSearchPosition;
        public bool Dirty;

        public FixedSizeTreePage(byte* b, int entrySize, int pageSize)
        {
            _ptr = b;
            _pageSize = pageSize;

            if (IsBranch)
                _entrySize = FixedSizeTree<TVal>.BranchEntrySize;
            else
                _entrySize = entrySize;
        }

        public void RefreshEntrySize()
        {
            _entrySize = IsBranch ? FixedSizeTree<TVal>.BranchEntrySize : LeafEntrySize;
            _tombstoneBitmapSize = 0;
        }

        private FixedSizeTreePageHeader* Header
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (FixedSizeTreePageHeader*)_ptr; }
        }

        public FixedSizeTreePageHeader PageHeader
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return *(FixedSizeTreePageHeader*)_ptr; }
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

        public bool HasTombstonesBitmap
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return (Header->TreeFlags & FixedSizeTreePageFlags.HasTombstonesBitmap) == FixedSizeTreePageFlags.HasTombstonesBitmap; }
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

        public int NumberOfActiveEntries
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return HasTombstonesBitmap ? Header->NumberOfEntries - Header->NumberOfTombstones : Header->NumberOfEntries; }
        }

        public int NumberOfTombstones
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return HasTombstonesBitmap ? Header->NumberOfTombstones : 0; }
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
        public void SetKey(TVal key, int position)
        {
            GetEntry(position)->SetKey(key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TVal GetKey(int position)
        {
            return GetEntry(Pointer + StartPosition, position, _entrySize)->GetKey<TVal>();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal FixedSizeTreeEntry* GetEntry(int position)
        {
            Debug.Assert(position >= 0 && ((position == 0 && NumberOfEntries == 0) || position < NumberOfEntries) ,$"FixedSizeTreePage: Requested an out of range entry {position} from [0-{NumberOfEntries-1}]");
            AssertEntrySizeMatchesThePage();
            return GetEntry(Pointer + StartPosition, position, _entrySize);
        }

        [Conditional("DEBUG")]
        private void AssertFitsNextToTombstonesBitmap()
        {
            int capacity = GetTombstonesLayout(_pageSize, LeafEntrySize).Capacity;

            Debug.Assert(NumberOfEntries <= capacity,
                $"FixedSizeTreePage: page {PageNumber} holds {NumberOfEntries} entries, more than the {capacity} that fit next to a tombstone bitmap");
        }

        [Conditional("DEBUG")]
        private void AssertEntrySizeMatchesThePage()
        {
            var expected = IsBranch ? FixedSizeTree<TVal>.BranchEntrySize : LeafEntrySize;
            if (_entrySize == expected)
                return;

            Debug.Fail($"FixedSizeTreePage: page {PageNumber} is a {FixedTreeFlags} with entries of {expected} bytes, but it was wrapped with entries of {_entrySize} bytes");
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
                NumberOfEntries * (IsLeaf ? _entrySize : FixedSizeTree<TVal>.BranchEntrySize));

            StartPosition = Constants.FixedSizeTree.PageHeaderSize;
        }

        public void RemoveEntry(int pos)
        {
            System.Diagnostics.Debug.Assert(pos >= 0 && pos < NumberOfEntries);
            System.Diagnostics.Debug.Assert(HasTombstonesBitmap == false, "Entries of a page with a tombstone bitmap have to be removed through AddTombstone / CompactTombstones");
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

        // -------------------------------------------------------------------------------------------------
        // Tombstones
        //
        // A converted leaf page reserves the tail of the page for a bitmap with one bit per entry slot, and
        // pins its entries at the page header (StartPosition never moves), so the bit index of an entry is
        // its position in the entries array. Deleting is then a single bit write instead of memmoving half
        // of the page, at the cost of a compaction once the page fills up or drops below the merge threshold.
        // -------------------------------------------------------------------------------------------------

        public static (int Capacity, int BitmapSize) GetTombstonesLayout(int pageSize, int entrySize)
        {
            int usableSpace = pageSize - Constants.FixedSizeTree.PageHeaderSize;
            int capacity = (usableSpace * 8) / (entrySize * 8 + 1);
            return (capacity, (capacity + 7) / 8);
        }

        private int LeafEntrySize
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Header->ValueSize + sizeof(long); }
        }

        private int TombstonesBitmapSize
        {
            get
            {
                if (_tombstoneBitmapSize == 0)
                    _tombstoneBitmapSize = GetTombstonesLayout(_pageSize, LeafEntrySize).BitmapSize;

                return _tombstoneBitmapSize;
            }
        }

        private byte* TombstonesBitmap
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return _ptr + _pageSize - TombstonesBitmapSize; }
        }

        public void InitializeTombstones()
        {
            Debug.Assert(IsLeaf, "Only leaf pages track tombstones");
            Debug.Assert(HasTombstonesBitmap == false, "Page already tracks tombstones");
            AssertFitsNextToTombstonesBitmap();
            AssertEntrySizeMatchesThePage();

            ResetStartPosition();

            ClearTombstones();

            Header->TreeFlags |= FixedSizeTreePageFlags.HasTombstonesBitmap;
        }

        public void ClearTombstones()
        {
            Memory.Set(_ptr + _pageSize - TombstonesBitmapSize, 0, TombstonesBitmapSize);
            Header->NumberOfTombstones = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsTombstoned(int position)
        {
            if (HasTombstonesBitmap == false)
                return false;

            Debug.Assert(position >= 0, $"FixedSizeTreePage: Requested the tombstone of a negative entry {position}");

            if (position >= NumberOfEntries)
                return false; // past the end of the entries, there is nothing there to tombstone

            return (TombstonesBitmap[position >> 3] & (byte)(1 << (position & 7))) != 0;
        }

        public void AddTombstone(int position)
        {
            Debug.Assert(HasTombstonesBitmap, "Page does not track tombstones");
            Debug.Assert(position >= 0 && position < NumberOfEntries);
            Debug.Assert(IsTombstoned(position) == false, $"Entry {position} in page {PageNumber} is already tombstoned");

            TombstonesBitmap[position >> 3] |= (byte)(1 << (position & 7));
            Header->NumberOfTombstones++;
        }

        public void RemoveTombstone(int position)
        {
            Debug.Assert(HasTombstonesBitmap, "Page does not track tombstones");
            Debug.Assert(position >= 0 && position < NumberOfEntries);
            Debug.Assert(IsTombstoned(position), $"Entry {position} in page {PageNumber} is not tombstoned");

            TombstonesBitmap[position >> 3] &= (byte)~(1 << (position & 7));
            Header->NumberOfTombstones--;
        }

        public void CompactTombstones()
        {
            Debug.Assert(HasTombstonesBitmap, "Page does not track tombstones");
            AssertEntrySizeMatchesThePage();

            if (Header->NumberOfTombstones == 0)
                return;

            var entries = Pointer + StartPosition;
            var entrySize = LeafEntrySize;
            var numberOfEntries = NumberOfEntries;
            var read = 0;
            var write = 0;

            while (read < numberOfEntries)
            {
                while (read < numberOfEntries && IsTombstoned(read))
                    read++;

                var runStart = read;
                while (read < numberOfEntries && IsTombstoned(read) == false)
                    read++;

                var runLength = read - runStart;
                if (runLength == 0)
                    continue;

                if (write != runStart)
                {
                    Memory.Move(entries + (write * entrySize),
                        entries + (runStart * entrySize),
                        runLength * entrySize);
                }

                write += runLength;
            }

            NumberOfEntries = (ushort)write;
            ClearTombstones();
        }

        public int CountActiveEntriesFrom(int position)
        {
            var numberOfEntries = NumberOfEntries;
            if (position >= numberOfEntries)
                return 0;

            return numberOfEntries - position - CountTombstones(position, numberOfEntries);
        }

        public int CountActiveEntriesBefore(int position)
        {
            if (position <= 0)
                return 0;

            return position - CountTombstones(0, position);
        }

        private int CountTombstones(int from, int toExclusive)
        {
            if (HasTombstonesBitmap == false || Header->NumberOfTombstones == 0 || from >= toExclusive)
                return 0;

            var bitmap = TombstonesBitmap;
            var firstByte = from >> 3;
            var lastByte = (toExclusive - 1) >> 3;

            var firstMask = 0xFF << (from & 7);
            var lastMask = 0xFF >> (7 - ((toExclusive - 1) & 7));

            if (firstByte == lastByte)
                return BitOperations.PopCount((uint)(bitmap[firstByte] & firstMask & lastMask));

            var count = BitOperations.PopCount((uint)(bitmap[firstByte] & firstMask & 0xFF));
            for (int i = firstByte + 1; i < lastByte; i++)
            {
                count += BitOperations.PopCount((uint)bitmap[i]);
            }

            return count + BitOperations.PopCount((uint)(bitmap[lastByte] & lastMask));
        }

        public int AdvanceToActiveEntry(int position, int count)
        {
            var numberOfEntries = NumberOfEntries;
            if (HasTombstonesBitmap == false || Header->NumberOfTombstones == 0)
                return Math.Min(position + count, numberOfEntries);

            for (; position < numberOfEntries; position++)
            {
                if (IsTombstoned(position))
                    continue;

                if (count-- == 0)
                    return position;
            }

            return numberOfEntries;
        }

        public int RetreatToActiveEntry(int position, int count)
        {
            if (count == 0)
                return position;

            if (HasTombstonesBitmap == false || Header->NumberOfTombstones == 0)
                return position - count;

            for (position--; position >= 0; position--)
            {
                if (IsTombstoned(position))
                    continue;

                if (--count == 0)
                    return position;
            }

            return 0;
        }
    }
}
