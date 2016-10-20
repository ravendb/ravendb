using System;
using System.Collections;
using System.Diagnostics;
using System.Numerics;
using Voron;
using Voron.Impl;

namespace Regression.PageLocator
{
    public class PageLocatorV2
    {
        private static readonly Vector<long> _indexes;


        private readonly LowLevelTransaction _tx;
        // This is the size of the cache, required to be _cacheSize % Vector<long>.Count == 0
        private readonly int _cacheSize;

        private readonly MyPage[] _cache;
        private readonly long[] _pages;
        private readonly bool[] _writeable;

        private int _current;

        static PageLocatorV2()
        {
            var indexes = new long[Vector<long>.Count];
            for (int i = 0; i < Vector<long>.Count; i++)
                indexes[i] = i + 1;

            _indexes = new Vector<long>(indexes);
        }

        public PageLocatorV2(LowLevelTransaction tx, int cacheSize = 4)
        {
            //Debug.Assert(tx != null);
            //Debug.Assert(cacheSize > 0);
            _tx = tx;

            if (tx != null)
                Debug.Fail("");

            // Align cache size to Vector<long>.Count
            _cacheSize = cacheSize;
            if (_cacheSize % Vector<long>.Count != 0)
                _cacheSize += Vector<long>.Count - cacheSize % Vector<long>.Count;

            _current = -1;
            _cache = new MyPage[_cacheSize];
            _writeable = new bool[_cacheSize];

            _pages = new long[_cacheSize];
            for (int i = 0; i < _pages.Length; i++)
                _pages[i] = -1;
        }

        public MyPage GetReadOnlyPage(long pageNumber)
        {
            var lookup = new Vector<long>(pageNumber);

            for (int i = 0; i < _cacheSize; i += Vector<long>.Count)
            {
                var pageNumbers = new Vector<long>(_pages, i);
                var comparison = Vector.Equals(pageNumbers, lookup);
                var result = Vector.ConditionalSelect(comparison, Vector<long>.One, Vector<long>.Zero);
                long index = Vector.Dot(_indexes, result);
                if (index != 0)
                    return _cache[i + index - 1];
            }

            // If we got here, there was a cache miss
            _current = (_current + 1) % _cacheSize;
            _cache[_current] = LowLevelTransactionStub.GetPage(pageNumber);
            _pages[_current] = pageNumber;
            _writeable[_current] = false;

            return _cache[_current];
        }

        public MyPage GetWritablePage(long pageNumber)
        {
           
            var lookup = new Vector<long>(pageNumber);

            for (int i = 0; i < _cacheSize; i += Vector<long>.Count)
            {
                var pageNumbers = new Vector<long>(_pages, i);
                var comparison = Vector.Equals(pageNumbers, lookup);
                var result = Vector.ConditionalSelect(comparison, Vector<long>.One, Vector<long>.Zero);
                long index = Vector.Dot(_indexes, result);
                if (index != 0)
                {
                    int j = i + (int)index - 1;
                    if (!_writeable[j])
                    {
                        _cache[j] = LowLevelTransactionStub.ModifyPage(pageNumber);
                        _writeable[j] = true;
                    }
                    return _cache[j];
                }
            }

            // If we got here, there was a cache miss
            _current = (_current + 1) % _cacheSize;
            _cache[_current] = LowLevelTransactionStub.ModifyPage(pageNumber);
            _pages[_current] = pageNumber;
            _writeable[_current] = true;

            return _cache[_current];
        }

        public void Clear()
        {
            _current = -1;
            Array.Clear(_cache, 0, _cache.Length);
            for (int i = 0; i < _pages.Length; i++)
                _pages[i] = -1;
        }

        public void Reset(long pageNumber)
        {
            var lookup = new Vector<long>(pageNumber);

            for (int i = 0; i < _cacheSize; i += Vector<long>.Count)
            {
                var pageNumbers = new Vector<long>(_pages, i);
                var comparison = Vector.Equals(pageNumbers, lookup);
                var result = Vector.ConditionalSelect(comparison, Vector<long>.One, Vector<long>.Zero);
                long index = Vector.Dot(_indexes, result);
                if (index != 0)
                {
                    int j = i + (int)index - 1;
                    _cache[j] = null;
                    _pages[j] = -1;

                    return;
                }
            }
        }
    }
}