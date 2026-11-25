using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;

namespace Voron.Data.BTrees
{
    [SkipLocalsInit]
    [StructLayout(LayoutKind.Sequential)]
    public unsafe struct FoundTreePageDescriptor
    {
        public const int MaxKeyStorage = 256;

        public byte* PagePointer;
        public long Number;

        public fixed byte KeyStorage[MaxKeyStorage];

        public SliceOptions FirstKeyOptions;
        public SliceOptions LastKeyOptions;

        public int FirstKeyLength;
        public int LastKeyLength;

        public Span<byte> FirstKey => MemoryMarshal.CreateSpan(ref KeyStorage[0], FirstKeyLength);
        public Span<byte> LastKey => MemoryMarshal.CreateSpan(ref KeyStorage[FirstKeyLength], LastKeyLength);
    }

    [SkipLocalsInit]
    public unsafe class RecentlyFoundTreePages()
    {
        // PERF: We are using a cache size that we can access directly from a 512 bits instruction when supported
        // or two 256 instructions.
        public const int CacheSize = 8;

        private Vector512<long> _pageNumberCache = Vector512<long>.AllBitsSet;
        private Vector256<uint> _pageGenerationCache = Vector256<uint>.Zero;

        private uint _current;
        private uint _currentGeneration = 1;

        private PageDescriptors _pageDescriptors;

        [InlineArray(CacheSize)]
        private struct PageDescriptors // avoid array heap allocation
        {
            private FoundTreePageDescriptor _element;
        }

        private int Compare(ReadOnlySpan<byte> x1, ReadOnlySpan<byte> y1)
        {
            int x1Length = x1.Length;
            int y1Length = y1.Length;
            var keyDiff = x1Length - y1Length;

            var r = Memory.CompareInline(x1, y1, Math.Min(x1Length, y1Length));
            return r != 0 ? r : keyDiff;
        }

        public bool TryFind(Slice key, out byte* pagePointer)
        {
            return TryFind(key.Options, key.AsReadOnlySpan(), out pagePointer);
        }

        public bool TryFind(SliceOptions keyOption, ReadOnlySpan<byte> key, out byte* pagePointer)
        {
            Debug.Assert(_currentGeneration > 0);

            uint location = FindMatchingByGeneration(_currentGeneration);

            int i = -1;
            while (location != 0)
            {
                // We will find the next matching item in the cache and advance the pointer to its rightful place.
                int advance = BitOperations.TrailingZeroCount(location) + 1;
                i += advance;

                // We will advance the bitmask to the location. If there are no more, it will be zero and fail
                // the re-entry check on the while loop. 
                location >>= advance;

                ref var current = ref _pageDescriptors[i];

                switch (keyOption)
                {
                    case SliceOptions.Key:
                        if (current.FirstKeyOptions != SliceOptions.BeforeAllKeys && Compare(key, current.FirstKey) < 0)
                            break;
                        if (current.LastKeyOptions != SliceOptions.AfterAllKeys && Compare(key, current.LastKey) > 0)
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
        private uint FindMatchingByGeneration(uint generation)
        {
            // PERF: We will check the entire small cache for current generation matches all at the same time
            // by comparing element-wise each item against a constant. Then since we will extract the most 
            // significant bit which will be set by the result of the .Equals() instruction and with that
            // we create a bitmask we will use during the search procedure.
            return Vector256.Equals(
                _pageGenerationCache,
                Vector256.Create(generation)
            ).ExtractMostSignificantBits();
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
                        _pageNumberCache,
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
                _pageDescriptors[slot].PagePointer = pagePointer;
        }

        public void Reset(long num)
        {
            int slot = FindSlot(num);
            if (slot == -1)
                return;

            // This just reset the cached page instance. 
            _pageGenerationCache = _pageGenerationCache.WithElement(slot, (uint)0);
            _pageNumberCache = _pageNumberCache.WithElement(slot, -1L);
        }

        // The slot of the page, or -1. Add reuses the slot of a page that is already in the cache, and Reset and Clear
        // set the page number of a slot they drop to -1, so a page is in at most one slot.
        private int FindSlot(long num)
        {
            Debug.Assert(_currentGeneration > 0);

            var matches = FindMatchingByPageNumber(num) & FindMatchingByGeneration(_currentGeneration);
            Debug.Assert(BitOperations.PopCount(matches) <= 1, $"page {num} is in more than one slot of the cache");

            return matches == 0 ? -1 : BitOperations.TrailingZeroCount(matches);
        }

        public void Clear()
        {
            _pageNumberCache = Vector512<long>.AllBitsSet;
            _pageGenerationCache = Vector256<uint>.Zero;

            _currentGeneration++;
            if (_currentGeneration == int.MaxValue)
                _currentGeneration = 1;

            Debug.Assert(FindMatchingByGeneration(_currentGeneration) == 0);
        }

        public void Add(TreePage page, SliceOptions firstKeyOption, ReadOnlySpan<byte> firstKey, SliceOptions lastKeyOption, ReadOnlySpan<byte> lastKey)
        {
            // PERF: Pages whose boundary keys don't fit in the struct are rejected outright, to avoid allocations.
            if (firstKey.Length + lastKey.Length > FoundTreePageDescriptor.MaxKeyStorage)
                return;

            Debug.Assert(_currentGeneration > 0);

            var currentPage = page.PageNumber;

            // If we already have the page in the cache (regardless of generation), we will overwrite it.
            // This would also ensure that every page whenever it was added to this cache, will have its
            // bucket assigned until it is removed completely.
            var match = FindMatchingByPageNumber(currentPage);

            // We will find the next matching item in the cache or get a new location.
            var position = match == 0 ? _current % CacheSize : (uint)BitOperations.TrailingZeroCount(match);

            ref var current = ref _pageDescriptors[(int)position];
            current.PagePointer = page.Base;
            current.Number = page.PageNumber;

            // We update the information regarding the keys to ensure we can get the Span<byte> representing it.
            current.FirstKeyLength = firstKey.Length;
            current.FirstKeyOptions = firstKeyOption;
            current.LastKeyLength = lastKey.Length;
            current.LastKeyOptions = lastKeyOption;

            if (firstKey.Length > 0)
            {
                Debug.Assert(current.FirstKey.Length == firstKey.Length);
                firstKey.CopyTo(current.FirstKey);
            }

            if (lastKey.Length > 0)
            {
                Debug.Assert(current.LastKey.Length == lastKey.Length);
                lastKey.CopyTo(current.LastKey);
            }

            _pageGenerationCache = _pageGenerationCache.WithElement((int)position, _currentGeneration);
            _pageNumberCache = _pageNumberCache.WithElement((int)position, page.PageNumber);

            _current++;

            Debug.Assert(FindMatchingByPageNumber(currentPage) != 0);
        }
    }
}
