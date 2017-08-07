// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System.Threading;

namespace Sparrow.Collections.LockFree
{
    internal sealed class DictionaryImplRef<TKey, TKeyStore, TValue>
            : DictionaryImpl<TKey, TKey, TValue>
                    where TKey : class
    {
        internal DictionaryImplRef(int capacity, ConcurrentDictionary<TKey, TValue> topDict)
            : base(capacity, topDict)
        {
        }

        internal DictionaryImplRef(int capacity, DictionaryImplRef<TKey, TKeyStore, TValue> other)
            : base(capacity, other)
        {
        }

        protected override bool TryClaimSlotForPut(ref TKey entryKey, TKey key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        protected override bool TryClaimSlotForCopy(ref TKey entryKey, TKey key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        private bool TryClaimSlot(ref TKey entryKey, TKey key)
        {
            var entryKeyValue = entryKey;
            if (entryKeyValue == null)
            {
                entryKeyValue = Interlocked.CompareExchange(ref entryKey, key, null);
                if (entryKeyValue == null)
                {
                    // claimed a new slot
                    allocatedSlotCount.Increment();
                    return true;
                }
            }

            return key == entryKeyValue || _keyComparer.Equals(key, entryKeyValue);
        }

        protected override DictionaryImpl<TKey, TKey, TValue> CreateNew(int capacity)
        {
            return new DictionaryImplRef<TKey, TKeyStore, TValue>(capacity, this);
        }
    }
}
