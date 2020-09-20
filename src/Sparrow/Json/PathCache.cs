using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Sparrow.Json
{
    public class PathCache
    {
        private int _used = -1;
        private readonly PathCacheHolder[] _items = new PathCacheHolder[512];

        private struct PathCacheHolder
        {
            public Dictionary<StringSegment, object> Path;
            public Dictionary<int, object> ByIndex;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void AcquirePathCache(out Dictionary<StringSegment, object> pathCache, out Dictionary<int, object> pathCacheByIndex)
        {
            // PERF: Avoids allocating gigabytes in FastDictionary instances on high traffic RW operations like indexing.
            if (_used >= 0)
            {
                var cache = _items[_used--];
                Debug.Assert(cache.Path != null);
                Debug.Assert(cache.ByIndex != null);

                pathCache = cache.Path;
                pathCacheByIndex = cache.ByIndex;

                return;
            }

            pathCache = new Dictionary<StringSegment, object>(StringSegmentEqualityStructComparer.BoxedInstance);
            pathCacheByIndex = new Dictionary<int, object>(NumericEqualityComparer.BoxedInstanceInt32);
        }

        public void ReleasePathCache(Dictionary<StringSegment, object> pathCache, Dictionary<int, object> pathCacheByIndex)
        {
            if (_used >= _items.Length - 1 || pathCache.Count >= 256) return;

            pathCache.Clear();
            pathCacheByIndex.Clear();

            _items[++_used] = new PathCacheHolder {ByIndex = pathCacheByIndex, Path = pathCache};
        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ClearUnreturnedPathCache()
        {
            for (var i = _used + 1; i < _items.Length; i++)
            {
                var cache = _items[i];

                //never allocated, no reason to continue seeking
                if (cache.Path == null)
                    break;

                //idly there shouldn't be unreleased path cache but we do have placed where we don't dispose of blittable object readers
                //and rely on the context.Reset to clear unwanted memory, but it didn't take care of the path cache.

                //Clear references for allocated cache paths so the GC can collect them.
                cache.ByIndex.Clear();
                cache.Path.Clear();
            }
        }
    }
}
