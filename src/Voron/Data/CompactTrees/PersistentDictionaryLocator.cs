using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Binary;
using Sparrow.Server;
using Voron.Impl;

namespace Voron.Data.CompactTrees
{
    public unsafe class PersistentDictionaryLocator
    {
        private struct DictionaryData
        {
            public long PageNumber;
            public PersistentDictionary Dictionary;
        }

        private const long Invalid = -1;

        private readonly DictionaryData[] _cache;
        private readonly int _andMask;

        public PersistentDictionaryLocator(int cacheSize)
        {
            Debug.Assert(cacheSize > 0);
            Debug.Assert(cacheSize <= 1024);

            if (!Bits.IsPowerOfTwo(cacheSize))
                cacheSize = Bits.PowerOf2(cacheSize);

            int shiftRight = Bits.CeilLog2(cacheSize);
            _andMask = (int)(0xFFFFFFFF >> (sizeof(uint) * 8 - shiftRight));

            _cache = new DictionaryData[cacheSize];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(long pageNumber, out PersistentDictionary dictionary)
        {
            var position = pageNumber & _andMask;

            ref var item = ref _cache[position];
            if (item.PageNumber == pageNumber)
            {
                // We will copy the reference and ensure again we have the right dictionary because there may be
                // multiple threads accessing the same locator at any time. 
                dictionary = item.Dictionary;
                return dictionary.PageNumber == pageNumber;
            }

            dictionary = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(long pageNumber, PersistentDictionary dictionary)
        {
            var position = pageNumber & _andMask;

            // No check need to be done at this level because at the read side we will
            // make sure to get the proper dictionary. 
            ref var item = ref _cache[position];
            item.Dictionary = dictionary;
            item.PageNumber = pageNumber;
        }
    }
}
