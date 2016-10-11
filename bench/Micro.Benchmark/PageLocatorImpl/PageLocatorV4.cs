using System;
using System.Collections;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Voron;
using Voron.Impl;

namespace Regression.PageLocator
{
    public class PageLocatorV4
    {
        private static readonly Vector<int> _indexes;

        private const ushort Invalid = unchecked((ushort)-1);

        private readonly LowLevelTransaction _tx;

        private readonly int _cacheSize;
        private readonly int[] _fingerprints;
        private readonly PageHandlePtrV3[] _cache;
        private readonly Vector<int> One;
        private readonly Vector<int> Zero;

        private int _current;

        static PageLocatorV4()
        {
            var indexes = new int[Vector<int>.Count];
            for (int i = 0; i < Vector<int>.Count; i++)
                indexes[i] = i + 1;

            _indexes = new Vector<int>(indexes);
        }

        public PageLocatorV4(LowLevelTransaction tx, int cacheSize = 4)
        {
            //Debug.Assert(tx != null);
            //Debug.Assert(cacheSize > 0);
            _tx = tx;

            if (tx != null)
                Debug.Fail("");

            // Align cache size to Vector<ushort>.Count
            _cacheSize = cacheSize;
            if (_cacheSize % Vector<int>.Count != 0)
                _cacheSize += Vector<int>.Count - cacheSize % Vector<int>.Count;

            _current = -1;
            _cache = new PageHandlePtrV3[_cacheSize];

            _fingerprints = new int[_cacheSize];
            for (int i = 0; i < _fingerprints.Length; i++)
                _fingerprints[i] = Invalid;

            One = new Vector<int>(1);
            Zero = new Vector<int>(0);
        }

        public MyPage GetReadOnlyPage(long pageNumber)
        {
            int fingerprint = (int)pageNumber;
            var lookup = new Vector<int>(fingerprint);
            int count = Vector<short>.Count;

            for (int i = 0; i < _cacheSize; i += count)
            {
                var pageNumbers = new Vector<int>(_fingerprints, i);

                if (Vector.EqualsAny(pageNumbers, lookup))
                {
                    for (int index = 0; index < count; index++)
                    {
                        int j = i + index;
                        if (_fingerprints[j] == fingerprint)
                        {
                            if (_cache[j].PageNumber == pageNumber)
                                return _cache[j].Value;

                            _cache[j] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.GetPage(pageNumber), false);
                            return _cache[j].Value;
                        }
                    }

                    throw new InvalidOperationException("This cant happen");
                }
            }         

            // If we got here, there was a cache miss
            _current = (_current + 1) % _cacheSize;
            _cache[_current] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.GetPage(pageNumber), false);
            _fingerprints[_current] = fingerprint;
            return _cache[_current].Value;
        }

        public MyPage GetWritablePage(long pageNumber)
        {
            int fingerprint = (int)pageNumber;
            var lookup = new Vector<int>(fingerprint);
            int count = Vector<short>.Count;

            for (int i = 0; i < _cacheSize; i += count)
            {
                var pageNumbers = new Vector<int>(_fingerprints, i);

                if (Vector.EqualsAny(pageNumbers, lookup))
                {
                    for (int index = 0; index < count; index++)
                    {
                        int j = i + index;
                        if (_fingerprints[j] == fingerprint)
                        {
                            if (_cache[j].PageNumber == pageNumber && _cache[j].IsWritable)
                                return _cache[j].Value;

                            _cache[j] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.ModifyPage(pageNumber), true);
                            return _cache[j].Value;
                        }
                    }

                    throw new InvalidOperationException("This cant happen");
                }
            }

            // If we got here, there was a cache miss
            _current = (_current + 1) % _cacheSize;
            _cache[_current] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.ModifyPage(pageNumber), true);
            _fingerprints[_current] = fingerprint;
            return _cache[_current].Value;
        }

        public void Clear()
        {
            _current = -1;
            Array.Clear(_cache, 0, _cache.Length);
            for (int i = 0; i < _fingerprints.Length; i++)
                _fingerprints[i] = Invalid;
        }

        public void Reset(long pageNumber)
        {
            int fingerprint = (int)pageNumber;
            var lookup = new Vector<int>(fingerprint);
            int count = Vector<short>.Count;

            for (int i = 0; i < _cacheSize; i += count)
            {
                var pageNumbers = new Vector<int>(_fingerprints, i);

                if (Vector.EqualsAny(pageNumbers, lookup))
                {
                    for (int index = 0; index < count; index++)
                    {
                        int j = i + index;
                        if (_fingerprints[j] == fingerprint && _cache[j].PageNumber == pageNumber)
                        {
                            _cache[j] = new PageHandlePtrV3();
                            _fingerprints[j] = Invalid;
                        }
                    }

                    throw new InvalidOperationException("This cant happen");
                }
            }
        }
    }
}