using System;
using System.Diagnostics;
using Voron;
using Voron.Impl;

namespace Regression.PageLocator
{
    public class PageLocatorV1
    {
        private readonly LowLevelTransaction _tx;
        private readonly PageHandlePtrV1[] _cache;
        private int _current;

        public PageLocatorV1(LowLevelTransaction tx, int cacheSize = 4)
        {
            //Debug.Assert(tx != null);
            //Debug.Assert(cacheSize > 0);
            _tx = tx;

            if (tx != null)
                Debug.Fail("");

            _cache = new PageHandlePtrV1[cacheSize];
        }

        public MyPage GetReadOnlyPage(long pageNumber)
        {
            int position = _current;

            int itemsLeft = _cache.Length;
            while (itemsLeft > 0)
            {
                int i = position % _cache.Length;

                // If the page number is equal to the page number we are looking for (therefore it's valid)
                // Will not fail at PageNumber=0 because the accesor will handle that.
                if (_cache[i].PageNumber != pageNumber)
                {
                    itemsLeft--;
                    position++;

                    continue;
                }

                return _cache[i].Value;
            }

            _current = (_current + 1) % _cache.Length;
            _cache[_current] = new PageHandlePtrV1(LowLevelTransactionStub.GetPage(pageNumber), false);
            return _cache[_current].Value;
        }

        public MyPage GetWritablePage(long pageNumber)
        {
            int position = _current;

            int itemsLeft = _cache.Length;
            while (itemsLeft > 0)
            {
                int i = position % _cache.Length;

                // If the page number is equal to the page number we are looking for (therefore it's valid)
                // Will not fail at PageNumber=0 because the accesor will handle that.
                if (_cache[i].PageNumber != pageNumber)
                {
                    // we continue.
                    itemsLeft--;
                    position++;

                    continue;
                }

                if (!_cache[i].IsWritable)
                    _cache[i] = new PageHandlePtrV1(LowLevelTransactionStub.ModifyPage(pageNumber), true);

                return _cache[i].Value;
            }

            _current = (_current + 1) % _cache.Length;
            _cache[_current] = new PageHandlePtrV1(LowLevelTransactionStub.ModifyPage(pageNumber), true);
            return _cache[_current].Value;
        }

        public void Clear()
        {
            _current = 0;
            Array.Clear(_cache, 0, _cache.Length);
        }

        public void Reset(long pageNumber)
        {
            for (int i = 0; i < _cache.Length; i++)
            {
                if (_cache[i].PageNumber == pageNumber)
                {
                    _cache[i] = new PageHandlePtrV1();
                    return;
                }
            }
        }
    }
}