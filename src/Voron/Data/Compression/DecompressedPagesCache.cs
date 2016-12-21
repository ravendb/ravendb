using System;
using System.Diagnostics;
using Voron.Impl;

namespace Voron.Data.Compression
{
    public class DecompressedPagesCache : IDisposable
    {
        public const int Size = 4;

        private readonly DecompressedLeafPage[] _cache;

        private int _current = 0;

        public DecompressedPagesCache()
        {
            _cache = new DecompressedLeafPage[Size];
        }

        public bool TryGet(long pageNumber, DecompressionUsage usage, out DecompressedLeafPage decompressed)
        {
            int position = _current;

            int itemsLeft = Size;
            while (itemsLeft > 0)
            {
                var page = _cache[position % Size];
                if (page == null)
                {
                    itemsLeft--;
                    position++;

                    continue;
                }

                if (page.PageNumber == pageNumber && page.Usage == usage)
                {
                    Debug.Assert(page.Cached);
                    
                    decompressed = page;

                    return true;
                }

                itemsLeft--;
                position++;
            }

            decompressed = null;
            return false;
        }

        public void Add(DecompressedLeafPage decompressed)
        {
            decompressed.Cached = true;

            var itemsLeft = Size;
            var position = _current + Size;

            DecompressedLeafPage old;
            while (itemsLeft > 0)
            {
                var itemIndex = position % Size;
                old = _cache[itemIndex];

                if (old == null)
                {
                    _cache[itemIndex] = decompressed;
                    return;
                }

                if (old.PageNumber == decompressed.PageNumber && old.Usage == decompressed.Usage)
                {
                    Debug.Assert(old != decompressed);

                    old.Cached = false;
                    old.Dispose();

                    _cache[itemIndex] = decompressed;
                    return;
                }

                itemsLeft--;
                position--;
            }

            _current = ++_current % Size;

            old = _cache[_current];

            if (old != null)
            {
                Debug.Assert(old != decompressed);

                old.Cached = false;
                old.Dispose();
            }

            _cache[_current] = decompressed;
        }

        public void Invalidate(long pageNumber, DecompressionUsage usage)
        {
            for (int i = 0; i < _cache.Length; i++)
            {
                var cached = _cache[i];
                if (cached != null && cached.PageNumber == pageNumber && cached.Usage == usage)
                {
                    cached.Cached = false;
                    cached.Dispose();

                    _cache[i] = null;

                    return;
                }
            }
        }

        public void Dispose()
        {
            for (var i = 0; i < _cache.Length; i++)
            {
                var item = _cache[i];

                if (item == null)
                    continue;

                item.Cached = false;
                item.Dispose();

                _cache[i] = null;
            }
        }

        public bool TryFindPageForReading(Slice key, LowLevelTransaction tx, out DecompressedLeafPage result)
        {
            Debug.Assert(key.Options == SliceOptions.Key);

            var position = _current;

            var itemsLeft = Size;
            while (itemsLeft > 0)
            {
                var page = _cache[position % Size];
                if (page == null || page.Usage != DecompressionUsage.Read || page.NumberOfEntries == 0) // decompressed page can has 0 entries if each compressed entry had a tombstone marker
                {
                    itemsLeft--;
                    position++;

                    continue;
                }

                Slice first;
                Slice last;

                using (page.GetNodeKey(tx, 0, out first))
                using (page.GetNodeKey(tx, page.NumberOfEntries - 1, out last))
                {
                    if (SliceComparer.Compare(key, first) >= 0 && SliceComparer.Compare(key, last) <= 0)
                    {
                        result = page;
                        return true;
                    }                    
                }

                itemsLeft--;
                position++;
            }

            result = null;
            return false;
        }
    }
}