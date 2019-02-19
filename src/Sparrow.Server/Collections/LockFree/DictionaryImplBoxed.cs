// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System.Threading;

namespace Sparrow.Server.Collections.LockFree
{
    internal sealed class DictionaryImplBoxed<TKey, TValue>
            : DictionaryImpl<TKey, Boxed<TKey>, TValue>
    {
        internal DictionaryImplBoxed(int capacity, LockFreeConcurrentDictionary<TKey, TValue> topDict)
            : base(capacity, topDict)
        {
        }

        internal DictionaryImplBoxed(int capacity, DictionaryImplBoxed<TKey, TValue> other)
            : base(capacity, other)
        {
        }

        protected override bool TryClaimSlotForPut(ref Boxed<TKey> entryKey, TKey key)
        {
            var entryKeyValue = entryKey;
            if (entryKeyValue == null)
            {
                entryKeyValue = Interlocked.CompareExchange(ref entryKey, new Boxed<TKey>(key), null);
                if (entryKeyValue == null)
                {
                    // claimed a new slot
                    this.allocatedSlotCount.Increment();
                    return true;
                }
            }

            return _keyComparer.Equals(key, entryKey.Value);
        }

        protected override bool TryClaimSlotForCopy(ref Boxed<TKey> entryKey,Boxed<TKey> key)
        {
            var entryKeyValue = entryKey;
            if (entryKeyValue == null)
            {
                entryKeyValue = Interlocked.CompareExchange(ref entryKey, key, null);
                if (entryKeyValue == null)
                {
                    // claimed a new slot
                    this.allocatedSlotCount.Increment();
                    return true;
                }
            }

            return _keyComparer.Equals(key.Value, entryKey.Value);
        }

        protected override bool keyEqual(TKey key, Boxed<TKey> entryKey)
        {
            //NOTE: slots are claimed in two stages - claim a hash, then set a key
            //      it is possible to observe a slot with a null key, but with hash already set
            //      that is not a match since the key is not yet in the table
            return entryKey != null && _keyComparer.Equals(key, entryKey.Value);
        }

        protected override DictionaryImpl<TKey, Boxed<TKey>, TValue> CreateNew(int capacity)
        {
            return new DictionaryImplBoxed<TKey, TValue>(capacity, this);
        }

        protected override TKey keyFromEntry(Boxed<TKey> entryKey)
        {
            return entryKey.Value;
        }
    }

    internal class Boxed<T>
    {
        public readonly T Value;

        public Boxed(T key)
        {
            this.Value = key;
        }
    }
}
