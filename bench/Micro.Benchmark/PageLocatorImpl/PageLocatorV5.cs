using System;
using System.Collections;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Voron;
using Voron.Impl;

namespace Regression.PageLocator
{
    /// <summary>
    /// Implements 4 loop unroll, uses ushorts rather than ints
    /// </summary>
    public class PageLocatorV5
    {
        private const ushort Invalid = 0;

        private readonly LowLevelTransaction _tx;

        private readonly int _cacheSize;
        private readonly ushort[] _fingerprints;
        private readonly PageHandlePtrV3[] _cache;

        private int _current;


        public PageLocatorV5(LowLevelTransaction tx, int cacheSize = 4)
        {
            //Debug.Assert(tx != null);
            //Debug.Assert(cacheSize > 0);
            _tx = tx;

            if (tx != null)
                Debug.Fail("");

            // Align cache size to 4 for loop unrolling
            _cacheSize = cacheSize;

            if (_cacheSize % 4 != 0)
            {
                _cacheSize += 4 - _cacheSize % 4;
            }

            _current = -1;
            _cache = new PageHandlePtrV3[_cacheSize];

            _fingerprints = new ushort[_cacheSize];
            for (ushort i = 0; i < _fingerprints.Length; i++)
                _fingerprints[i] = Invalid;
        }

        public MyPage GetReadOnlyPage(long pageNumber)
        {
            ushort sfingerprint = (ushort)pageNumber;
            int fingerprint = sfingerprint;

            int i = 0;
            int size = _cacheSize;
            while (i < size)
            {
                int f1 = _fingerprints[i + 0];
                int f2 = _fingerprints[i + 1];
                int f3 = _fingerprints[i + 2];
                int f4 = _fingerprints[i + 3];

                // This is used to force the JIT to layout the code as if unlikely() compiler directive existed.
                if (f1 == fingerprint)
                    goto Found;

                if (f2 == fingerprint)
                {
                    i += 1; goto Found;
                }

                if (f3 == fingerprint)
                {
                    i += 2; goto Found;
                }

                if (f4 == fingerprint)
                {
                    i += 1; goto Found;
                }

                i += 4;
            }


            // If we got here, there was a cache miss
            _current = (_current + 1) % _cacheSize;
            _cache[_current] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.GetPage(pageNumber), false);
            _fingerprints[_current] = sfingerprint;

            return _cache[_current].Value;

            Found:
            // This is not the common case on the loop and we are returning anyways. It doesnt matter the jump is far.
            if (_cache[i].PageNumber == pageNumber)
                return _cache[i].Value;

            _cache[i] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.GetPage(pageNumber), false);
            return _cache[i].Value;
        }

        public MyPage GetWritablePage(long pageNumber)
        {
            ushort sfingerprint = (ushort)pageNumber;
            int fingerprint = sfingerprint;

            int i = 0;
            int size = _cacheSize;
            while (i < size)
            {
                int f1 = _fingerprints[i + 0];
                int f2 = _fingerprints[i + 1];
                int f3 = _fingerprints[i + 2];
                int f4 = _fingerprints[i + 3];

                // This is used to force the JIT to layout the code as if unlikely() compiler directive existed.
                if (f1 == fingerprint)
                    goto Found;

                if (f2 == fingerprint)
                {
                    i += 1; goto Found;
                }

                if (f3 == fingerprint)
                {
                    i += 2; goto Found;
                }

                if (f4 == fingerprint)
                {
                    i += 1; goto Found;
                }

                i += 4;
            }


            // If we got here, there was a cache miss
            _current = (_current + 1) % _cacheSize;
            _cache[_current] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.ModifyPage(pageNumber), true);
            _fingerprints[_current] = sfingerprint;
            return _cache[_current].Value;

            Found:
            if (_cache[i].PageNumber == pageNumber && _cache[i].IsWritable)
                return _cache[i].Value;

            _cache[i] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.ModifyPage(pageNumber), true);
            return _cache[i].Value;
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
            ushort sfingerprint = (ushort)pageNumber;
            int fingerprint = sfingerprint;

            int i = 0;
            int size = _cacheSize;
            while (i < size)
            {
                int f1 = _fingerprints[i + 0];
                int f2 = _fingerprints[i + 1];
                int f3 = _fingerprints[i + 2];
                int f4 = _fingerprints[i + 3];

                // This is used to force the JIT to layout the code as if unlikely() compiler directive existed.
                if (f1 == fingerprint)
                    goto Found;

                if (f2 == fingerprint)
                {
                    i += 1; goto Found;
                }

                if (f3 == fingerprint)
                {
                    i += 2; goto Found;
                }

                if (f4 == fingerprint)
                {
                    i += 1; goto Found;
                }

                i += 4;
            }

            return;

            Found:
            if (_cache[i].PageNumber == pageNumber)
            {
                _cache[i] = new PageHandlePtrV3();
                _fingerprints[i] = Invalid;
            }
        }
    }
}