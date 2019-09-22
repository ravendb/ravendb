// -----------------------------------------------------------------------
//  <copyright file="BinaryTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Sparrow.Server;

namespace Voron.Data.BTrees
{
    public class RecentlyFoundTreePages
    {
        public class FoundTreePage : IDisposable
        {
            public readonly long Number;
            public readonly Slice FirstKey;
            public readonly Slice LastKey;
            public readonly long[] CursorPath;
            private ByteStringContext.Scope _firstScope;
            private ByteStringContext.Scope _lastScope;

            public TreePage Page;

            public FoundTreePage(long number, TreePage page, Slice firstKey, Slice lastKey, long[] cursorPath, ByteStringContext.Scope firstScope, ByteStringContext.Scope lastScope)
            {
                Number = number;
                Page = page;
                FirstKey = firstKey;
                LastKey = lastKey;
                CursorPath = cursorPath;
                _firstScope = firstScope;
                _lastScope = lastScope;
            }

            public void Dispose()
            {
                _firstScope.Dispose();
               _lastScope.Dispose();
            }
        }

        private readonly FoundTreePage[] _cache;

        private readonly int _cacheSize;

        private int _current = 0;

        public RecentlyFoundTreePages(int cacheSize)
        {
            _cache = new FoundTreePage[cacheSize];
            _cacheSize = cacheSize;
        }

        public void Add(FoundTreePage page)
        {
            int itemsLeft = _cacheSize;
            int position = _current + _cacheSize;
            while (itemsLeft > 0)
            {
                var itemIndex = position % _cacheSize;
                var item = _cache[itemIndex];
                if (item == null || item.Number == page.Number)
                {
                    item?.Dispose();
                    _cache[itemIndex] = page;
                    return;
                }

                itemsLeft--;
                position--;
            }

            _current = (++_current) % _cacheSize;
            _cache[_current]?.Dispose();
            _cache[_current] = page;
        }

        public FoundTreePage Find(Slice key)
        {
            int position = _current;

            int itemsLeft = _cacheSize;
            while ( itemsLeft > 0 )
            {
                var page = _cache[position % _cacheSize];
                if (page == null)
                {
                    itemsLeft--;
                    position++;

                    continue;
                }

                var first = page.FirstKey;
                var last = page.LastKey;

                switch (key.Options)
                {
                    case SliceOptions.Key:
                        if ((first.Options != SliceOptions.BeforeAllKeys && SliceComparer.Compare(key, first) < 0))
                            break;
                        if (last.Options != SliceOptions.AfterAllKeys && SliceComparer.Compare(key, last) > 0)
                            break;
                        return page;
                    case SliceOptions.BeforeAllKeys:
                        if (first.Options == SliceOptions.BeforeAllKeys)
                            return page;
                        break;
                    case SliceOptions.AfterAllKeys:
                        if (last.Options == SliceOptions.AfterAllKeys)
                            return page;
                        break;
                    default:
                        throw new ArgumentException(key.Options.ToString());
                }

                itemsLeft--;
                position++;
            }

            return null;
        }

        public void Clear()
        {
            for (var i = 0; i < _cacheSize; i++)
            {
                var page = _cache[i];
                page?.Dispose();
            }
            
            Array.Clear(_cache, 0, _cacheSize);
        }

        public void Reset(long num)
        {
            for (int i = 0; i < _cache.Length; i++)
            {
                var page = _cache[i];
                if (page != null && page.Number == num)
                {
                    page.Page = null;
                    return;
                }
            }
        }
    }
}
