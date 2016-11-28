using System.Diagnostics;

namespace Voron.Data.Compression
{
    public class DecompressedPagesCache
    {
        private readonly DecompressedLeafPage[] _cache;
        private readonly int _cacheSize;

        private int _current = 0;

        public DecompressedPagesCache(int cacheSize = 8)
        {
            _cache = new DecompressedLeafPage[cacheSize];
            _cacheSize = cacheSize;
        }

        public bool TryGet(long pageNumber, ushort version, out DecompressedLeafPage decompressed)
        {
            int position = _current;

            int itemsLeft = _cacheSize;
            while (itemsLeft > 0)
            {
                var page = _cache[position % _cacheSize];
                if (page == null)
                {
                    itemsLeft--;
                    position++;

                    continue;
                }

                if (page.PageNumber == pageNumber && page.Version == version)
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

            var itemsLeft = _cacheSize;
            var position = _current + _cacheSize;

            DecompressedLeafPage old;
            while (itemsLeft > 0)
            {
                var itemIndex = position % _cacheSize;
                old = _cache[itemIndex];

                if (old == null)
                {
                    _cache[itemIndex] = decompressed;
                    return;
                }

                if (old.PageNumber == decompressed.PageNumber && old.Version == decompressed.Version)
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

            _current = ++_current % _cacheSize;

            old = _cache[_current];

            if (old != null)
            {
                Debug.Assert(old != decompressed);

                old.Cached = false;
                old.Dispose();
            }

            _cache[_current] = decompressed;
        }

        public void Invalidate(long pageNumber, ushort version)
        {
            for (int i = 0; i < _cache.Length; i++)
            {
                var cached = _cache[i];
                if (cached != null && cached.PageNumber == pageNumber && cached.Version == version)
                {
                    cached.Cached = false;
                    cached.Dispose();

                    _cache[i] = null;

                    return;
                }
            }
        }
    }
}