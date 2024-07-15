using System;
using System.Buffers;
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
        public const int MaxCursorPath = 8;
        public const int MaxKeyStorage = 256;

        public TreePage Page;
        public long Number;

        // We are setting ourselves to not allow pages whose path sequences are longer than MaxCursorPath, this makes sense
        // because if we are in such a long sequence, it is highly unlikely this cache would be useful anyway. 
        public fixed long PathSequence[MaxCursorPath];

        public fixed byte KeyStorage[MaxKeyStorage];

        public SliceOptions FirstKeyOptions;
        public SliceOptions LastKeyOptions;

        public int PathLength;
        public int FirstKeyLength;
        public int LastKeyLength;

        public ReadOnlySpan<byte> FirstKey => MemoryMarshal.CreateSpan(ref KeyStorage[0], FirstKeyLength);
        public ReadOnlySpan<byte> LastKey => MemoryMarshal.CreateSpan(ref KeyStorage[FirstKeyLength], LastKeyLength);

        public ReadOnlySpan<long> Cursor => MemoryMarshal.CreateReadOnlySpan(ref PathSequence[0], PathLength);

        public void SetFirstKey(ReadOnlySpan<byte> key, SliceOptions option)
        {
            FirstKeyLength = key.Length;
            FirstKeyOptions = option;

            Debug.Assert(FirstKey.Length == key.Length);

            if (key.Length <= 0)
                return;

            Unsafe.CopyBlock(ref KeyStorage[0], in key[0], (uint)key.Length);
        }

        public void SetLastKey(ReadOnlySpan<byte> key, SliceOptions option)
        {
            LastKeyLength = key.Length;
            LastKeyOptions = option;

            Debug.Assert(LastKey.Length == key.Length);

            if (key.Length <= 0)
                return;

            Unsafe.CopyBlock(ref KeyStorage[FirstKeyLength], in key[0], (uint)key.Length);
        }

        public void SetCursor(ReadOnlySpan<long> cursorPath)
        {
            PathLength = cursorPath.Length;
            if (cursorPath.Length <= 0)
                return;

            Span<byte> pathSequence = MemoryMarshal.Cast<long, byte>(MemoryMarshal.CreateSpan(ref PathSequence[0], MaxCursorPath));
            Unsafe.CopyBlock(ref pathSequence[0], in MemoryMarshal.Cast<long, byte>(cursorPath)[0], (uint)cursorPath.Length * sizeof(long));
        }

        public int CompareFirstKey(ReadOnlySpan<byte> key)
        {
            int x1Length = key.Length;
            int y1Length = FirstKeyLength;

            if (x1Length == 0)
                return x1Length - y1Length;

            ref readonly byte firstKeyStart = ref KeyStorage[0];
            ref readonly byte keyStart = ref key[0];
            var r = Memory.CompareInline(in keyStart, in firstKeyStart, Math.Min(x1Length, y1Length));

            return r != 0 ? r : x1Length - y1Length;
        }

        public int CompareLastKey(ReadOnlySpan<byte> key)
        {
            int x1Length = key.Length;
            int y1Length = LastKeyLength;

            if (x1Length == 0)
                return x1Length - y1Length;

            ref readonly byte firstKeyStart = ref KeyStorage[FirstKeyLength];
            ref readonly byte keyStart = ref key[0];
            var r = Memory.CompareInline(in keyStart, in firstKeyStart, Math.Min(x1Length, y1Length));

            return r != 0 ? r : x1Length - y1Length;
        }
    }

    [SkipLocalsInit]
    public unsafe class RecentlyFoundTreePages
    {
        // PERF: We are using a cache size that we can access directly from a 512 bits instruction when supported
        // or two 256 instructions.
        public const int CacheSize = 8;

        private Vector512<long> _pageNumberCache = Vector512<long>.AllBitsSet;
        private Vector256<uint> _pageGenerationCache = Vector256<uint>.Zero;

        private uint _current;
        private uint _currentGeneration = 1;

        private readonly FoundTreePageDescriptor[] _pageDescriptors = new FoundTreePageDescriptor[CacheSize];

        public bool TryFind(Slice key, out FoundTreePageDescriptor foundPage)
        {
            return TryFind(key.Options, key.AsReadOnlySpan(), out foundPage);
        }

        public bool TryFind(SliceOptions keyOption, ReadOnlySpan<byte> key, out FoundTreePageDescriptor foundPage)
        {
            Debug.Assert(_currentGeneration > 0);
            Debug.Assert(_pageDescriptors.Length == CacheSize);

            var descriptors = _pageDescriptors;

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

                ref var current = ref descriptors[i];

                switch (keyOption)
                {
                    case SliceOptions.Key:
                        if (current.FirstKeyOptions != SliceOptions.BeforeAllKeys && current.CompareFirstKey(key) < 0)
                            break;
                        if (current.LastKeyOptions != SliceOptions.AfterAllKeys && current.CompareLastKey(key) > 0)
                            break;

                        foundPage = current;
                        return true;
                    case SliceOptions.BeforeAllKeys:
                        if (current.FirstKeyOptions == SliceOptions.BeforeAllKeys)
                        {
                            foundPage = current;
                            return true;
                        }
                        break;
                    case SliceOptions.AfterAllKeys:
                        if (current.LastKeyOptions == SliceOptions.AfterAllKeys)
                        {
                            foundPage = current;
                            return true;
                        }

                        break;
                }
            }

            Unsafe.SkipInit(out foundPage);
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

        public void Reset(long num)
        {
            Debug.Assert(_currentGeneration > 0);
            Debug.Assert(_pageDescriptors.Length == CacheSize);

            var matchesByGeneration = FindMatchingByGeneration(_currentGeneration);
            var matchesByPageNumber = FindMatchingByPageNumber(num);

            var matches = matchesByPageNumber & matchesByGeneration;

            int i = -1;
            while (matches != 0)
            {
                // We will find the next matching item in the cache and advance the pointer to its rightful place.
                int advance = BitOperations.TrailingZeroCount(matches) + 1;
                i += advance;

                Debug.Assert(_pageNumberCache[i] == num);

                // We will advance the bitmask to the location. If there are no more, it will be zero and fail
                // the re-entry check on the while loop. 
                matches >>= advance;

                // This just reset the cached page instance. 
                _pageGenerationCache = _pageGenerationCache.WithElement(i, (uint)0);
                _pageNumberCache = _pageNumberCache.WithElement(i, -1L);
            }

            Debug.Assert((FindMatchingByPageNumber(num) & FindMatchingByGeneration(_currentGeneration)) == 0);
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

        public void Add(TreePage page, SliceOptions firstKeyOption, ReadOnlySpan<byte> firstKey, SliceOptions lastKeyOption, ReadOnlySpan<byte> lastKey, ReadOnlySpan<long> cursorPath)
        {
            // PERF: The idea behind this check is that IF the page has a bigger cursor path than what we support
            // on the struct, and this been a performance improvement we are going to reject it outright. We will
            // not try to accomodate variability in this regard in order to avoid allocations.
            if (cursorPath.Length > FoundTreePageDescriptor.MaxCursorPath ||
                firstKey.Length + lastKey.Length > FoundTreePageDescriptor.MaxKeyStorage)
                return;

            Debug.Assert(_currentGeneration > 0);
            Debug.Assert(_pageDescriptors.Length == CacheSize);

            var currentPage = page.PageNumber;

            // If we already have the page in the cache (regardless of generation), we will overwrite it.
            // This would also ensure that every page whenever it was added to this cache, will have its
            // bucket assigned until it is removed completely.
            var match = FindMatchingByPageNumber(currentPage);

            // We will find the next matching item in the cache or get a new location.
            var position = match == 0 ? _current % CacheSize : (uint)BitOperations.TrailingZeroCount(match);

            ref var current = ref _pageDescriptors[(int)position];
            current.Page = page;
            current.Number = page.PageNumber;

            // We update the information regarding the keys to ensure we can get the Span<byte> representing it.
            current.SetFirstKey(firstKey, firstKeyOption);
            current.SetLastKey(lastKey, lastKeyOption);
            current.SetCursor(cursorPath);

            _pageGenerationCache = _pageGenerationCache.WithElement((int)position, _currentGeneration);
            _pageNumberCache = _pageNumberCache.WithElement((int)position, page.PageNumber);

            _current++;

            Debug.Assert(FindMatchingByPageNumber(currentPage) != 0);
        }
    }
}
