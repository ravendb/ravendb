using System;
using System.Diagnostics;
using System.Diagnostics.Contracts;
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

        // WARNING: We are putting the Key Storage at the start because we want it to be safe to do an unmasked load
        //          using SIMD operations and still be in the boundary of the object (even if we are reading garbage)
        //          in order to optimize the key comparison. 
        public fixed byte KeyStorage[MaxKeyStorage];

        // We are setting ourselves to not allow pages whose path sequences are longer than MaxCursorPath, this makes sense
        // because if we are in such a long sequence, it is highly unlikely this cache would be useful anyway. 
        public fixed long PathSequence[MaxCursorPath];

        public TreePage Page;
        public long Number;

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
            FirstKeyOptions = option;

            if (key.Length <= 0)
            {
                Debug.Assert(option != SliceOptions.Key, "There is no such thing as a key with 0 length.");

                FirstKeyLength = sizeof(uint);
                uint placeholderKey = option == SliceOptions.BeforeAllKeys ? 0 : 0xFFFFFFFF;
                Unsafe.WriteUnaligned(ref KeyStorage[0], placeholderKey);
                return;
            }

            FirstKeyLength = key.Length;
            Unsafe.CopyBlock(ref KeyStorage[0], in key[0], (uint)key.Length);
        }

        public void SetLastKey(ReadOnlySpan<byte> key, SliceOptions option)
        {
            LastKeyOptions = option;

            if (key.Length <= 0)
            {
                Debug.Assert(option != SliceOptions.Key, "There is no such thing as a key with 0 length.");

                LastKeyLength = sizeof(uint);
                uint placeholderKey = option == SliceOptions.BeforeAllKeys ? 0 : 0xFFFFFFFF;
                Unsafe.WriteUnaligned(ref KeyStorage[FirstKeyLength], placeholderKey);
                return;
            }

            LastKeyLength = key.Length;
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
            Debug.Assert(_currentGeneration > 0);
            Debug.Assert(_pageDescriptors.Length == CacheSize);

            // We do not require to initialize this.
            Unsafe.SkipInit(out foundPage);

            uint location = FindMatchingByGeneration(_currentGeneration);
            if (location == 0)
                return false; // Early skip, we don't need to do anything
            
            var keyOption = key.Options;
            if (keyOption == SliceOptions.Key)
            {
                if (key.Size < Vector128<byte>.Count || key.Size > Vector256<byte>.Count)
                {
                    return TryFindReference(location, key, out foundPage);
                }

                return TryFindVector256(location, key, out foundPage);
            }

            return TryFindNoKey(location, keyOption, out foundPage);
        }

        private bool TryFindReference(uint location, ReadOnlySpan<byte> key, out FoundTreePageDescriptor foundPage)
        {
            // PERF: We know this is for keys smaller than 128 bits (16 bytes) so we are going to be using that fact.
            var descriptors = _pageDescriptors;

            ref byte keyStart = ref Unsafe.AsRef(in key[0]);

            int i = -1;
            while (location != 0)
            {
                // We will find the next matching item in the cache and advance the pointer to its rightful place.
                int advance = BitOperations.TrailingZeroCount(location) + 1;
                i += advance;

                // We will advance the bitmask to the location. If there are no more, it will be zero and fail
                // the re-entry check on the while loop. 
                location >>= advance;

                ref readonly var current = ref descriptors[i];

                if (current.FirstKeyOptions != SliceOptions.BeforeAllKeys)
                {
                    var r = Memory.CompareSmallInlineNet7(in keyStart, in current.KeyStorage[0], Math.Min(current.FirstKeyLength, key.Length));
                    r = r != 0 ? r : key.Length - current.FirstKeyLength;
                    if (r < 0)
                        continue;
                }

                if (current.LastKeyOptions != SliceOptions.AfterAllKeys)
                {
                    var r = Memory.CompareSmallInlineNet7(in keyStart, in current.KeyStorage[current.FirstKeyLength], Math.Min(current.LastKeyLength, key.Length));
                    r = r != 0 ? r : key.Length - current.LastKeyLength;
                    if (r > 0)
                        continue;
                }

                foundPage = current;
                return true;
            }

            Unsafe.SkipInit(out foundPage);
            return false;
        }

        private static ReadOnlySpan<byte> LoadTable256 => new byte[]
        {
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
        };

        [SkipLocalsInit]
        private bool TryFindVector256(uint location, ReadOnlySpan<byte> key, out FoundTreePageDescriptor foundPage)
        {
            var descriptors = _pageDescriptors;

            // We are going to look at the actual memory allocated, if it is big enough
            // we are going to use the fingerprint method anyways, even if it is garbage

            int loadShift = key.Length - Vector128<byte>.Count;
            ref byte keyStart = ref Unsafe.AsRef(in key[0]);
            
            var fingerprint = Vector256.Create(
                Vector128.LoadUnsafe(in keyStart),
                Vector128.ShiftLeft(Vector128.LoadUnsafe(ref Unsafe.Add(ref keyStart, loadShift)), Vector128<byte>.Count - loadShift)
            );

            int x1Length = key.Length;
            
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
                ref byte lastKeyStart = ref current.KeyStorage[current.FirstKeyLength];

                if (current.FirstKeyOptions != SliceOptions.BeforeAllKeys)
                {
                    var y1FirstLength = Math.Min(x1Length, current.FirstKeyLength);
                    var firstMask = Vector256.LoadUnsafe(in MemoryMarshal.AsRef<byte>(LoadTable256), (uint)(Vector256<byte>.Count - Math.Min(y1FirstLength, Vector256<byte>.Count)));

                    var firstMatches = (uint)PortableIntrinsics.MoveMask(
                        Vector256.Equals(
                            Vector256.BitwiseAnd(fingerprint, firstMask),
                            Vector256.BitwiseAnd(Vector256.LoadUnsafe(ref current.KeyStorage[0]), firstMask)
                    ));

                    // We invert matches to find differences, which are found in the bit-flag.
                    // We then add offset of first difference to the keys in order to check that specific byte.
                    var firstBytesToAdvance = BitOperations.TrailingZeroCount(~firstMatches);
                    int firstCmp = firstMatches == uint.MaxValue ? 0 : key[firstBytesToAdvance] - current.KeyStorage[firstBytesToAdvance];
                    firstCmp = firstCmp == 0 && y1FirstLength > 0 ? x1Length - y1FirstLength : firstCmp;
                    if (firstCmp < 0)
                        continue;
                }

                if (current.LastKeyOptions != SliceOptions.AfterAllKeys)
                {
                    var y1LastLength = Math.Min(x1Length, current.LastKeyLength);
                    var maskLastLength = Math.Min(y1LastLength, Vector256<byte>.Count);
                    var lastMask = Vector256.LoadUnsafe(in MemoryMarshal.AsRef<byte>(LoadTable256), (uint)(Vector256<byte>.Count - maskLastLength));

                    var lastMatches = (uint)PortableIntrinsics.MoveMask(
                        Vector256.Equals(
                            Vector256.BitwiseAnd(fingerprint, lastMask),
                            Vector256.BitwiseAnd(Vector256.LoadUnsafe(in lastKeyStart), lastMask)
                    ));

                    // We invert matches to find differences, which are found in the bit-flag.
                    // We then add offset of first difference to the keys in order to check that specific byte.
                    var lastBytesToAdvance = BitOperations.TrailingZeroCount(~lastMatches);
                    int lastCmp = lastMatches == uint.MaxValue ? 0 : key[lastBytesToAdvance] - current.KeyStorage[current.FirstKeyLength + lastBytesToAdvance];
                    lastCmp = lastCmp == 0 && y1LastLength > 0 ? x1Length - y1LastLength : lastCmp;
                    if (lastCmp > 0)
                        continue;
                }
                
                foundPage = current;
                return true;
            }

            Unsafe.SkipInit(out foundPage);
            return false;
        }

        [SkipLocalsInit]
        private bool TryFindNoKey(uint location, SliceOptions keyOption, out FoundTreePageDescriptor foundPage)
        {
            Debug.Assert(_currentGeneration > 0);
            Debug.Assert(_pageDescriptors.Length == CacheSize);

            var descriptors = _pageDescriptors;

            int i = -1;
            while (location != 0)
            {
                // We will find the next matching item in the cache and advance the pointer to its rightful place.
                int advance = BitOperations.TrailingZeroCount(location) + 1;
                i += advance;

                // We will advance the bitmask to the location. If there are no more, it will be zero and fail
                // the re-entry check on the while loop. 
                location >>= advance;

                ref readonly var current = ref descriptors[i];

                var firstKeyOption = current.FirstKeyOptions;
                var lastKeyOption = current.LastKeyOptions;

                switch (keyOption, firstKeyOption, lastKeyOption)
                {
                    case (SliceOptions.BeforeAllKeys, SliceOptions.BeforeAllKeys, _):
                    case (SliceOptions.AfterAllKeys, _, SliceOptions.AfterAllKeys):
                        foundPage = current;
                        return true;
                    default:
                        continue;
                }
            }

            Unsafe.SkipInit(out foundPage);
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
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

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
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

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public void Add(TreePage page, SliceOptions firstKeyOption, ReadOnlySpan<byte> firstKey, SliceOptions lastKeyOption, ReadOnlySpan<byte> lastKey, ReadOnlySpan<long> cursorPath)
        {
            // PERF: The idea behind this check is that IF the page has a bigger cursor path than what we support
            // on the struct, and this been a performance improvement we are going to reject it outright. We will
            // not try to accomodate variability in this regard in order to avoid allocations.
            if (cursorPath.Length > FoundTreePageDescriptor.MaxCursorPath ||
                firstKey.Length + lastKey.Length + sizeof(uint) * 2 > FoundTreePageDescriptor.MaxKeyStorage)
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
