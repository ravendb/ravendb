using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Sparrow.Binary;
using Sparrow.Server;
using Voron.Impl;

namespace Voron.Data.CompactTrees
{
    public sealed unsafe class PersistentDictionaryLocator
    {
        private readonly object[] _cache;
        private readonly int _andMask;

        public PersistentDictionaryLocator(int cacheSize)
        {
            Debug.Assert(cacheSize > 0);
            Debug.Assert(cacheSize <= 1024);

            if (!Bits.IsPowerOfTwo(cacheSize))
                cacheSize = Bits.PowerOf2(cacheSize);

            int shiftRight = Bits.CeilLog2(cacheSize);
            _andMask = (int)(0xFFFFFFFF >> (sizeof(uint) * 8 - shiftRight));

            _cache = new object[cacheSize];
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryGet(long dictionaryId, out PersistentDictionary dictionary)
        {
            var position = dictionaryId & _andMask;

            switch (_cache[position])
            {
                case PersistentDictionary dic:
                    dictionary = dic;
                    return dic.DictionaryId == dictionaryId;
                case Dictionary<long, PersistentDictionary> dictionaries:
                    return dictionaries.TryGetValue(dictionaryId, out dictionary);
            }
            dictionary = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Set(long pageNumber, PersistentDictionary dictionary)
        {
            var position = pageNumber & _andMask;

            // This is intentionally racy because we are trying to optimize the TryGet method
            // as much as possible, we are fine with "losing" cached instances, they will make themselves
            // up over time and fix themselves

            // No check need to be done at this level because at the read side we will
            // make sure to get the proper dictionary. 
            switch (_cache[position])
            {
                case null:
                    _cache[position] = dictionary;
                    break;
                case PersistentDictionary existing:
                    if (existing.DictionaryId == dictionary.DictionaryId)
                        return; // should never happen
                    _cache[position] = new Dictionary<long, PersistentDictionary>
                    {
                        [existing.DictionaryId] = existing,
                        [dictionary.DictionaryId] = dictionary
                    };
                    break;
                case Dictionary<long, PersistentDictionary> dictionaries:
                    _cache[position] = new Dictionary<long, PersistentDictionary>(dictionaries)
                    {
                        [dictionary.DictionaryId] = dictionary
                    };
                    break;
            }
        }
    }
}
