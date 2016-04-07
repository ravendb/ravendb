using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Voron.Impl;

namespace Voron
{
    public class PageLocator
    {
        private readonly LowLevelTransaction _tx;
        private readonly PageHandlePtr[] _cache;
        private int current = 0;

        public PageLocator ( LowLevelTransaction tx, int cacheSize = 4)
        {
            Debug.Assert(tx != null);

            this._tx = tx;
            this._cache = new PageHandlePtr[cacheSize];
        }

        public Page GetReadOnlyPage (long pageNumber)
        {
            int position = current;

            int itemsLeft = _cache.Length;
            while (itemsLeft > 0)
            {
                int i = position % _cache.Length;

                // If the value is not valid or the page number is not equal
                if (!_cache[i].IsValid || _cache[i].PageNumber != pageNumber)
                {
                    // we continue.
                    itemsLeft--;
                    position++;

                    continue;
                }

                return _cache[i].Value;
            }

            current = (++current) % _cache.Length;
            _cache[current] = new PageHandlePtr(_tx.GetPage(pageNumber), false);
            return _cache[current].Value;
        }

        private const int Invalid = -1;

        public Page GetWritablePage(long pageNumber)
        {
            int position = current;

            int itemsLeft = _cache.Length;
            while (itemsLeft > 0)
            {
                int i = position % _cache.Length;

                // If the value is not valid or the page number is not equal
                if (!_cache[i].IsValid || _cache[i].PageNumber != pageNumber)
                {
                    // we continue.
                    itemsLeft--;
                    position++;

                    continue;
                }

                if (!_cache[i].IsWritable)
                    _cache[i] = new PageHandlePtr(_tx.ModifyPage(pageNumber), true);

                return _cache[i].Value;
            }

            current = (++current) % _cache.Length;
            _cache[current] = new PageHandlePtr(_tx.ModifyPage(pageNumber), true);
            return _cache[current].Value;
        }

        public void Clear ()
        {
            Array.Clear(_cache, 0, _cache.Length);            
        }

        public void Reset(long pageNumber)
        {
            for (int i = 0; i < _cache.Length; i++)
            {
                if (_cache[i].IsValid && _cache[i].PageNumber == pageNumber)
                {
                    _cache[i] = new PageHandlePtr();
                    return;
                }
            }
        }
    }
}
