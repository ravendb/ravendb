using System;
using System.Diagnostics;
using Sparrow.Server;
using Sparrow.Threading;
using Voron.Impl;

namespace Micro.Benchmark.PageLocatorImpl
{
    /// <summary>
    /// Uses an allocator (i.e. unmanaged memory). Gets rid of bound checking
    /// </summary>
    public unsafe class PageLocatorV6
    {
        private const ushort Invalid = 0;

        private readonly ByteStringContext _allocator = new ByteStringContext(SharedMultipleUseFlag.None);
        private readonly LowLevelTransaction _tx;

        private readonly int _cacheSize;
        private readonly ushort* _fingerprints;
        private readonly PageHandlePtrV3[] _cache;

        private int _current;


        public PageLocatorV6(LowLevelTransaction tx, int cacheSize = 4)
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

            _allocator.Allocate(_cacheSize * sizeof(ushort), out var fingerprint);
            _fingerprints = (ushort*)fingerprint.Ptr;
            for (ushort i = 0; i < _cacheSize; i++)
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
                if (f1 == fingerprint) goto Found;

                if (f2 == fingerprint) goto Found1;

                if (f3 == fingerprint) goto Found2;

                if (f4 == fingerprint) goto Found3;

                i += 4;
            }


            // If we got here, there was a cache miss
            _current = (_current + 1) % _cacheSize;
            _cache[_current] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.GetPage(pageNumber), false);
            _fingerprints[_current] = sfingerprint;

            return _cache[_current].Value;

            Found1: i += 1; goto Found;
            Found2: i += 2; goto Found;
            Found3: i += 3;
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
                if (f1 == fingerprint) goto Found;

                if (f2 == fingerprint) goto Found1;

                if (f3 == fingerprint) goto Found2;

                if (f4 == fingerprint) goto Found3;

                i += 4;
            }


            // If we got here, there was a cache miss
            _current = (_current + 1) % _cacheSize;
            _cache[_current] = new PageHandlePtrV3(pageNumber, LowLevelTransactionStub.ModifyPage(pageNumber), true);
            _fingerprints[_current] = sfingerprint;

            return _cache[_current].Value;

            Found1: i += 1; goto Found;
            Found2: i += 2; goto Found;
            Found3: i += 3;
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
            for (int i = 0; i < _cacheSize; i++)
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
                if (f1 == fingerprint) goto Found;

                if (f2 == fingerprint) goto Found1;

                if (f3 == fingerprint) goto Found2;

                if (f4 == fingerprint) goto Found3;

                i += 4;
            }

            return;

            Found1: i += 1; goto Found;
            Found2: i += 2; goto Found;
            Found3: i += 3;
            Found:
            if (_cache[i].PageNumber == pageNumber)
            {
                _cache[i] = new PageHandlePtrV3();
                _fingerprints[i] = Invalid;
            }
        }
    }
}
