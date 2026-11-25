using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Voron.Data.BTrees
{
    // Lives in memory of the transaction's allocator, aligned to 64 bytes, see Tree.AllocateRecentlyFoundPages, so it starts
    // with Initialize. The page numbers are one vector, the small fields of all slots share two cache lines and the keys of each
    // slot start on a cache line. 1,792 bytes, so with the alignment it fits the allocator's 2 KB blocks.
    [SkipLocalsInit]
    [StructLayout(LayoutKind.Explicit, Size = Size)]
    public unsafe struct RecentlyFoundTreePages
    {
        // PERF: We are using a cache size that we can access directly from a 512 bits instruction when supported
        // or two 256 instructions.
        public const int CacheSize = 8;

        // the first and last key of a page together, a page whose keys are longer is not cached
        public const int MaxKeyStorage = 192;

        private const int KeysOffset = 256;
        private const int Size = KeysOffset + CacheSize * MaxKeyStorage;

        // the page number in each slot, -1 for an empty slot
        [FieldOffset(0)]
        private Vector512<long> _pageNumbers;

        [FieldOffset(64)]
        private uint _current;

        [FieldOffset(72)]
        private Slots _slots;

        [FieldOffset(KeysOffset)]
        private fixed byte _keys[CacheSize * MaxKeyStorage];

        private struct Slot
        {
            public byte* PagePointer;
            public ushort FirstKeyLength;
            public ushort LastKeyLength;
            public SliceOptions FirstKeyOptions;
            public SliceOptions LastKeyOptions;
        }

        [InlineArray(CacheSize)]
        private struct Slots
        {
            private Slot _element;
        }

        public void Initialize()
        {
            Debug.Assert(sizeof(RecentlyFoundTreePages) == Size && sizeof(Slot) == 16);

            _pageNumbers = Vector512<long>.AllBitsSet;
            _current = 0;
        }

        private Span<byte> FirstKey(int slot) => MemoryMarshal.CreateSpan(ref _keys[slot * MaxKeyStorage], _slots[slot].FirstKeyLength);

        private Span<byte> LastKey(int slot) =>
            MemoryMarshal.CreateSpan(ref _keys[slot * MaxKeyStorage + _slots[slot].FirstKeyLength], _slots[slot].LastKeyLength);

        public bool TryFind(Slice key, out byte* pagePointer)
        {
            return TryFind(key.Options, key.AsReadOnlySpan(), out pagePointer);
        }

        public bool TryFind(SliceOptions keyOption, ReadOnlySpan<byte> key, out byte* pagePointer)
        {
            uint location = FindUsedSlots();
            while (location != 0)
            {
                int i = BitOperations.TrailingZeroCount(location);
                location &= location - 1;

                ref var current = ref _slots[i];

                switch (keyOption)
                {
                    case SliceOptions.Key:
                        if (current.FirstKeyOptions != SliceOptions.BeforeAllKeys && key.SequenceCompareTo(FirstKey(i)) < 0)
                            break;
                        if (current.LastKeyOptions != SliceOptions.AfterAllKeys && key.SequenceCompareTo(LastKey(i)) > 0)
                            break;

                        pagePointer = current.PagePointer;
                        return true;
                    case SliceOptions.BeforeAllKeys:
                        if (current.FirstKeyOptions == SliceOptions.BeforeAllKeys)
                        {
                            pagePointer = current.PagePointer;
                            return true;
                        }
                        break;
                    case SliceOptions.AfterAllKeys:
                        if (current.LastKeyOptions == SliceOptions.AfterAllKeys)
                        {
                            pagePointer = current.PagePointer;
                            return true;
                        }

                        break;
                    default:
                        throw new ArgumentException(keyOption.ToString());
                }
            }

            pagePointer = null;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private uint FindUsedSlots()
        {
            // PERF: We check all the slots of the cache at the same time by comparing element-wise each page number
            // against -1, the value of an empty slot. The most significant bit of each element of the result gives
            // one bit per slot, and inverting them gives the bitmask of the used slots that the search goes through.
            return (uint)~Vector512.Equals(_pageNumbers, Vector512<long>.AllBitsSet).ExtractMostSignificantBits() & 0xFF;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private uint FindMatchingByPageNumber(long pageNumber)
        {
            // PERF: We will check the entire small cache for current page number matches all at the same time
            // by comparing element-wise each item against a constant. Then since we will extract the most 
            // significant bit which will be set by the result of the .Equals() instruction and with that
            // we create a bitmask we will use during the search procedure. In this case we can actually coerce
            // to uint because we know there are no more than 8 elements in the cache.

            return (uint)Vector512.Equals(
                        _pageNumbers,
                        Vector512.Create(pageNumber)
                    ).ExtractMostSignificantBits();
        }

        /// <summary>
        /// Points the entry of a page that was just modified to its writable copy. Only valid when the modification
        /// keeps the page's keys inside the range the entry was created for (no split, merge or compression).
        /// </summary>
        public void Refresh(long num, byte* pagePointer)
        {
            int slot = FindSlot(num);
            if (slot != -1)
                _slots[slot].PagePointer = pagePointer;
        }

        public void Reset(long num)
        {
            int slot = FindSlot(num);
            if (slot != -1)
                _pageNumbers = _pageNumbers.WithElement(slot, -1L);
        }

        // The slot of the page, or -1. Add reuses the slot of a page that is already in the cache, so a page is in at most one slot.
        private int FindSlot(long num)
        {
            Debug.Assert(num >= 0);

            var matches = FindMatchingByPageNumber(num);
            Debug.Assert(BitOperations.PopCount(matches) <= 1, $"page {num} is in more than one slot of the cache");

            return matches == 0 ? -1 : BitOperations.TrailingZeroCount(matches);
        }

        public void Clear()
        {
            _pageNumbers = Vector512<long>.AllBitsSet;
        }

        public void Add(TreePage page, SliceOptions firstKeyOption, ReadOnlySpan<byte> firstKey, SliceOptions lastKeyOption, ReadOnlySpan<byte> lastKey)
        {
            // PERF: Pages whose boundary keys don't fit in the struct are rejected outright, to avoid allocations.
            if (firstKey.Length + lastKey.Length > MaxKeyStorage)
                return;

            var pageNumber = page.PageNumber;

            // A page that is in the cache already keeps its slot, so a page is in at most one slot.
            var match = FindMatchingByPageNumber(pageNumber);
            var position = match == 0 ? (int)(_current % CacheSize) : BitOperations.TrailingZeroCount(match);

            ref var current = ref _slots[position];
            current.PagePointer = page.Base;
            current.FirstKeyOptions = firstKeyOption;
            current.FirstKeyLength = (ushort)firstKey.Length;
            current.LastKeyOptions = lastKeyOption;
            current.LastKeyLength = (ushort)lastKey.Length;

            firstKey.CopyTo(FirstKey(position));
            lastKey.CopyTo(LastKey(position));

            _pageNumbers = _pageNumbers.WithElement(position, pageNumber);

            _current++;

            Debug.Assert(FindMatchingByPageNumber(pageNumber) != 0);
        }
    }
}
