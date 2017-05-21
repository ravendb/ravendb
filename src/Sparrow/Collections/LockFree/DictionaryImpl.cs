// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Sparrow.Collections.LockFree
{
    internal abstract class DictionaryImpl
    {
        internal enum ValueMatch
        {
            Any,            // sets new value unconditionally, used by index set
            NullOrDead,     // set value if original value is null or dead, used by Add/TryAdd
            NotNullOrDead,  // set value if original value is alive, used by Remove
            OldValue,       // sets new value if old value matches
        }

        internal sealed class Prime
        {
            internal object originalValue;

            public Prime(object originalValue)
            {
                this.originalValue = originalValue;
            }
        }

        internal static readonly object TOMBSTONE = new object();
        internal static readonly Prime TOMBPRIME = new Prime(TOMBSTONE);
        internal static readonly object NULLVALUE = new object();

        // represents forcefully dead entry 
        // we insert it in old table during rehashing
        // to reduce chances that more entries are added
        protected const int TOMBPRIMEHASH = 1 << 31;

        // we cannot distigush zero keys from uninitialized state
        // so we force them to have this special hash instead
        protected const int ZEROHASH = 1 << 30;

        // all regular hashes have these bits set
        // to be different from 0, TOMBPRIMEHASH or ZEROHASH
        protected const int REGULAR_HASH_BITS = TOMBPRIMEHASH | ZEROHASH;

        protected const int REPROBE_LIMIT = 4;
        protected const int REPROBE_LIMIT_SHIFT = 1;
        // Heuristic to decide if we have reprobed toooo many times.  Running over
        // the reprobe limit on a 'get' call acts as a 'miss'; on a 'put' call it
        // can trigger a table resize.  Several places must have exact agreement on
        // what the reprobe_limit is, so we share it here.
        // NOTE: Not static for perf reasons    
        //       (some JITs insert useless code related to generics if this is a static)
        protected static int ReprobeLimit(int lenMask)
        {
            // 1/2 of table with some extra
            return REPROBE_LIMIT + (lenMask >> REPROBE_LIMIT_SHIFT);
        }

        protected static bool EntryValueNullOrDead(object entryValue)
        {
            return entryValue == null || entryValue == TOMBSTONE;
        }

        protected static int ReduceHashToIndex(int fullHash, int lenMask)
        {
            fullHash = fullHash & ~REGULAR_HASH_BITS;
            var h2 = fullHash << 1;
            if ((uint)h2 <= (uint)lenMask)
                return h2;

            return MixAndMask((uint)fullHash, lenMask);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int MixAndMask(uint h, int lenMask)
        {
            h = Hashing.Mix(h);

            h &= (uint)lenMask;

            return (int)h;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static object ToObjectValue<TValue>(TValue value)
        {
            if (default(TValue) == null)
            {
                return (object)value ?? NULLVALUE;
            }

            return (object)value;
        }

        internal static DictionaryImpl<TKey, TValue> CreateRef<TKey, TValue>(ConcurrentDictionary<TKey, TValue> topDict, int capacity)
            where TKey : class
        {
            var result = new DictionaryImplRef<TKey, TKey, TValue>(capacity, topDict);
            return result;
        }
    }
}
